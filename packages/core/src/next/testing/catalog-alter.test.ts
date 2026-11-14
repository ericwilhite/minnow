/** PG authority for stable-id ADD/RENAME/DROP and frozen attmissingval across storage forms. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_ALTER_REACH, createReach } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { rowsOf } from "../write/test-support.js";
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
for (const seed of seedsFor("next-catalog-alter", [0xb786, 0xb787]))
  it(`keeps stable columns and frozen missing values seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native ALTER", CATALOG_ALTER_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.constantFrom("renamed", "Case", "a.b", 'a"b'),
          fc.integer({ min: -100, max: 100 }),
          async (persisted, name, value) => {
            reach.beginHistory();
            await pg.exec("DROP TABLE IF EXISTS stage2_t");
            const core = await new StorageCore(4).open();
            try {
              const ddl = "CREATE TABLE stage2_t(id INTEGER PRIMARY KEY, a INTEGER)";
              await pg.exec(ddl);
              await core.execute(ddl);
              await pg.exec(`INSERT INTO stage2_t VALUES (1,${String(value)}),(2,NULL)`);
              await core.writeBatch("insertBatch", "stage2_t", [
                { id: 1, a: value },
                { id: 2, a: null },
              ]);
              const old = core.harness.path.pin(),
                table = old.catalog.tableByName("stage2_t");
              if (!table) throw new Error("Missing original table");
              try {
                if (persisted) {
                  await core.harness.path.quiesce();
                  reach.reach("published");
                } else reach.reach("resident");
                for (const sql of [
                  `ALTER TABLE stage2_t ADD COLUMN "frozen.key" INTEGER NOT NULL DEFAULT ${String(value + 1)}`,
                  `ALTER TABLE stage2_t RENAME COLUMN a TO ${quote(name)}`,
                ]) {
                  await pg.exec(sql);
                  await core.execute(sql);
                }
                expect(await rowsOf(old, table.id)).toEqual([
                  [1, value],
                  [2, null],
                ]);
                reach.reach("old snapshot");
                expect(core.harness.path.catalog.table(table.id)?.columns[1]?.id).toBe(
                  table.columns[1]?.id,
                );
                const select = "SELECT * FROM stage2_t ORDER BY id";
                const result = await core.execute(select);
                if (!("batches" in result)) throw new Error("Missing ALTER result");
                expect(oracleRows(result)).toEqual((await pg.query(select)).rows);
                reach.reach("backfill");
                reach.reach("rename");
                await core.writeBatch("insertBatch", "stage2_t", [
                  { id: 3, [name]: value + 2, "frozen.key": value + 3 },
                ]);
                await pg.exec(
                  `INSERT INTO stage2_t VALUES (3,${String(value + 2)},${String(value + 3)})`,
                );
                const drop = `ALTER TABLE stage2_t DROP COLUMN ${quote(name)}`;
                await pg.exec(drop);
                await core.execute(drop);
                reach.reach("drop");
              } finally {
                old.release();
              }
              await core.reopen();
              const result = await core.execute("SELECT * FROM stage2_t ORDER BY id");
              if (!("batches" in result)) throw new Error("Missing recovered result");
              expect(oracleRows(result)).toEqual(
                (await pg.query("SELECT * FROM stage2_t ORDER BY id")).rows,
              );
              reach.reach("reopen");
              const current = core.harness.path.catalog.encode(),
                tx = await core.harness.path.begin(),
                statement = tx.statement({
                  rules: () => ({ check: () => null, generate: () => undefined }),
                });
              const lowered = core.lowerer.lower(
                "ALTER TABLE stage2_t ADD COLUMN rollback_only INTEGER",
                core.context(),
              );
              if (lowered.kind !== "ddl" || !lowered.change)
                throw new Error("Missing rollback ALTER");
              await statement.changeCatalog(lowered.change);
              statement.rollback();
              tx.rollback();
              // Rollback burns a catalog version, but restores the definitions.
              expect(
                core.harness.path.catalog.tables[0]?.columns.some(
                  (column) => column.name === "rollback_only",
                ),
              ).toBe(false);
              expect(core.harness.path.catalog.encode()).toEqual(current);
              reach.reach("rollback");
            } finally {
              await core.close();
            }
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [false, "a.b", 2],
            [true, "Case", 3],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
