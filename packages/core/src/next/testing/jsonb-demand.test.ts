/** A JSON growth checkpoint does not turn LIMIT into eager validation of later rows. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSONB_DEMAND_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
it.each(seedsFor("next-jsonb-demand", [0xb68c, 0xb68d]))(
  "keeps demand and row-error order through growth (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSONB lazy demand", JSONB_DEMAND_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 5 }),
        fc.integer({ min: 0, max: 2 }),
        fc.integer({ min: 5000, max: 7000 }),
        fc.boolean(),
        async (shape, limit, exponent, early) => {
          reach.beginHistory();
          const document = early ? "[1,]" : `{"n":1e${String(exponent)}}`;
          let body: string;
          if (shape === 0)
            body = `SELECT doc::jsonb ->> 'n' AS j FROM(VALUES(1,'${document}'::text),(2,'[1,]'::text),(3,'{"n":3}'::text)) q(id,doc)`;
          else if (shape === 1)
            body = `SELECT id FROM(VALUES(1,'{"n":null}'::text),(2,'${document}'::text),(3,'[1,]'::text)) q(id,doc) WHERE doc::jsonb ->> 'n' IS NOT NULL`;
          else if (shape === 4)
            body = `SELECT id FROM(SELECT id,doc::jsonb AS j FROM(VALUES(1,'{"n":null}'::text),(2,'${document}'::text),(3,'[1,]'::text)) q(id,doc)) s WHERE j ->> 'n' IS NOT NULL`;
          else if (shape === 5)
            body = `SELECT id FROM(SELECT id,10/id AS n,doc::jsonb AS j FROM(VALUES(1,'{"n":null}'::text),(2,'${document}'::text),(0,'{"n":3}'::text)) q(id,doc)) s WHERE n>0 AND j ->> 'n' IS NOT NULL`;
          else if (shape === 3)
            body = `SELECT id FROM(SELECT id,10/id AS n,doc FROM(VALUES(1,'{"n":null}'::text),(2,'${document}'::text),(0,'[1,]'::text)) q(id,doc)) s WHERE n>0 AND doc::jsonb ->> 'n' IS NOT NULL`;
          else
            body = `SELECT n,doc::jsonb ->> 'n' AS j FROM(SELECT 1/id AS n,doc FROM(VALUES(1,'${document}'::text),(0,'{"n":2}'::text)) q(id,doc)) s`;
          const sql = body + ` LIMIT ${String(limit)}`;
          let authority: unknown;
          try {
            authority = { rows: (await pg.query(sql)).rows };
          } catch (error) {
            authority = { state: (error as { code: string }).code };
          }
          let actual: unknown;
          try {
            const result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("Expected JSON rows");
            actual = { rows: oracleRows(result) };
          } catch (error) {
            actual = { state: (error as { sqlState: string }).sqlState };
          }
          expect(actual, sql).toEqual(authority);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach(`limit ${String(limit)}`);
          reach.reach(
            [
              "project",
              "filter",
              "producer error",
              "producer filter error",
              "nested JSON error",
              "nested data error",
            ][shape] ?? "project",
          );
          if (!early && limit === 1) reach.reach("growth before demand satisfied");
          if (early && limit > 0) reach.reach("early error");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [0, 0, 5000, false],
          [0, 1, 5000, false],
          [0, 2, 5000, false],
          [1, 1, 6000, false],
          [2, 1, 7000, false],
          [2, 2, 5000, true],
          [3, 1, 5000, false],
          [4, 1, 5000, false],
          [4, 2, 5000, false],
          [5, 1, 5000, false],
          [5, 2, 5000, false],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
