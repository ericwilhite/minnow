/** Stored views bind dependencies even in CTEs ordinary execution prunes. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_VIEW_PRUNING_REACH, createReach } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { LoweredDatabase } from "./lowering-harness.js";
const state = async (work: () => unknown) => {
  try {
    await work();
    return null;
  } catch (error) {
    if (error && typeof error === "object") {
      if ("sqlState" in error) return error.sqlState;
      if ("code" in error) return error.code;
    }
    throw error;
  }
};
for (const seed of seedsFor("next-catalog-view-pruning", [0xb79a, 0xb79b]))
  it(`binds pruned dependency declarations without changing execution seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("view dependency pruning", CATALOG_VIEW_PRUNING_REACH);
    try {
      await pg.exec(
        "CREATE TYPE e AS ENUM ('x'); CREATE SEQUENCE s; CREATE TABLE t(n INTEGER, extra INTEGER)",
      );
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 4 }),
          fc.boolean(),
          fc.boolean(),
          async (shape, materialized, replace) => {
            reach.beginHistory();
            const native = new LoweredDatabase();
            for (const sql of [
              "CREATE TYPE e AS ENUM ('x')",
              "CREATE SEQUENCE s",
              "CREATE TABLE t(n INTEGER, extra INTEGER)",
            ])
              native.execute(sql);
            const c = `c AS ${materialized ? "MATERIALIZED " : ""}(SELECT 'x'::e AS v,nextval('s') AS n FROM t WHERE t.n>0)`;
            const body =
              shape === 0
                ? `WITH ${c} SELECT 1 AS n`
                : shape === 1
                  ? `WITH ${c},d AS (SELECT * FROM c) SELECT 1 AS n`
                  : shape === 2
                    ? `SELECT n FROM (WITH ${c} SELECT 1 AS n) q`
                    : shape === 3
                      ? "WITH RECURSIVE c(v,n) AS (SELECT 'x'::e,1::bigint UNION ALL SELECT c.v,nextval('s') FROM c JOIN t ON t.n=c.n WHERE c.n<3) SELECT 1 AS n"
                      : `WITH ${c} SELECT * FROM c`;
            expect(native.execute(body)).toEqual((await pg.query(body)).rows);
            if (replace) {
              const initial =
                shape === 4
                  ? "CREATE VIEW v AS SELECT 'x'::e AS v,1::bigint AS n WHERE false"
                  : "CREATE VIEW v AS SELECT 9 AS n";
              await pg.exec(initial);
              native.execute(initial);
            }
            const oldId = native.catalog.view.views.find((v) => v.name === "v")?.id;
            const create = `CREATE ${replace ? "OR REPLACE " : ""}VIEW v AS ${body}`;
            try {
              await pg.exec(create);
              native.execute(create);
              expect(native.execute("SELECT * FROM v")).toEqual(
                (await pg.query("SELECT * FROM v")).rows,
              );
              if (replace) {
                expect(native.catalog.view.views.find((v) => v.name === "v")?.id).toBe(oldId);
                reach.reach("replacement identity");
              }
              for (const drop of [
                "DROP TYPE e",
                "DROP SEQUENCE s",
                "ALTER TABLE t DROP COLUMN n",
              ]) {
                const authority = await state(() => pg.exec(drop));
                expect(authority).toBe("2BP01");
                expect(await state(() => native.execute(drop))).toBe(authority);
              }
              // Column-level binding must not turn this into a dependency on every column.
              await pg.exec("ALTER TABLE t DROP COLUMN extra");
              native.execute("ALTER TABLE t DROP COLUMN extra");
              expect(native.execute(body)).toEqual((await pg.query(body)).rows);
              reach.reach(
                [
                  "plain unused",
                  "unused referrer",
                  "nested unused",
                  "recursive unused",
                  "used CTE admitted",
                ][shape] as (typeof CATALOG_VIEW_PRUNING_REACH)[number],
              );
              if (materialized && shape < 3) reach.reach("materialized");
              reach.reach("ordinary query preserved");
              reach.reach("column precision");
            } finally {
              await pg.exec(
                "DROP VIEW IF EXISTS v; ALTER TABLE t ADD COLUMN IF NOT EXISTS extra INTEGER",
              );
            }
          },
        ),
        {
          seed,
          numRuns: 10,
          examples: [
            [0, true, false],
            [1, false, false],
            [2, false, false],
            [3, false, false],
            [4, false, false],
            [0, false, true],
            [4, false, true],
          ],
        },
      );
      reach.assertReached();
      // Ordinary native commit/checkpoint/reopen must preserve the metadata-only reads too.
      const core = await new StorageCore().open();
      try {
        for (const sql of [
          "CREATE TYPE e AS ENUM ('x')",
          "CREATE SEQUENCE s",
          "CREATE TABLE t(n INTEGER)",
          "CREATE VIEW v AS WITH RECURSIVE c(v,n) AS (SELECT 'x'::e,1::bigint UNION ALL SELECT c.v,nextval('s') FROM c JOIN t ON t.n=c.n WHERE c.n<3) SELECT 1 AS n",
        ])
          await core.execute(sql);
        await core.harness.path.quiesce();
        await core.reopen();
        const result = await core.execute("SELECT * FROM v");
        if (!("batches" in result)) throw new Error("Missing view rows");
        expect(oracleRows(result)).toEqual([{ n: 1 }]);
        for (const drop of ["DROP TYPE e", "DROP SEQUENCE s", "ALTER TABLE t DROP COLUMN n"])
          expect(await state(() => core.execute(drop))).toBe("2BP01");
      } finally {
        await core.close();
      }
    } finally {
      await pg.close();
    }
  }, 120_000);
