/** FILTER rejects a row before either transition arguments or aggregate ORDER BY are evaluated. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_AGGREGATE_FILTER_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
  const ddl = "CREATE TABLE filter_json(n INTEGER PRIMARY KEY,d INTEGER,v TEXT,keep BOOLEAN)";
  await pg.exec(ddl);
  await core.execute(ddl);
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
async function outcome(sql: string, postgres: boolean): Promise<unknown> {
  try {
    if (postgres) return { rows: (await pg.query(sql)).rows };
    const result = await core.execute(sql);
    if (!("batches" in result)) throw new Error("Expected JSON rows");
    return { rows: oracleRows(result) };
  } catch (error) {
    if (error instanceof Error && "sqlState" in error) return { state: error.sqlState };
    if (error instanceof Error && "code" in error) return { state: error.code };
    throw error;
  }
}
it.each(seedsFor("next-json-aggregate-filter-errors", [0xb716, 0xb717]))(
  "skips invalid value, key and ordering expressions only on rejected rows (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "JSON aggregate FILTER evaluation",
      JSON_AGGREGATE_FILTER_REACH,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 5 }),
        fc.constantFrom(true, false, null),
        fc.integer({ min: 2, max: 8 }),
        fc.nat(31),
        fc.constantFrom("bad", '["\\u0000"]', "1e131072"),
        fc.boolean(),
        async (shape, keep, n, position, bad, filterFault) => {
          reach.beginHistory();
          const at = position % n;
          await pg.exec("TRUNCATE filter_json");
          await core.execute("DROP TABLE filter_json");
          await core.execute(
            "CREATE TABLE filter_json(n INTEGER PRIMARY KEY,d INTEGER,v TEXT,keep BOOLEAN)",
          );
          const rows = Array.from({ length: n }, (_, i) => ({
            n: i,
            d: i === at ? 0 : 1,
            v: i === at ? bad : String(i),
            keep: i === at ? keep : true,
          }));
          await core.writeBatch("insertBatch", "filter_json", rows);
          for (const row of rows)
            await pg.query("INSERT INTO filter_json VALUES($1,$2,$3,$4)", [
              row.n,
              row.d,
              row.v,
              row.keep,
            ]);
          const calls = [
            "json_agg(v::jsonb ORDER BY n)",
            "jsonb_agg(v::jsonb ORDER BY n)",
            "json_object_agg(n/d,n ORDER BY n)",
            "jsonb_object_agg(n/d,n ORDER BY n)",
            "json_agg(n ORDER BY n/d)",
            "jsonb_agg(n ORDER BY n/d)",
          ];
          const predicate = filterFault ? "CASE WHEN keep THEN n/d>0 ELSE keep END" : "keep";
          const sql = `SELECT ${calls[shape] ?? "json_agg(v::jsonb ORDER BY n)"} FILTER(WHERE ${predicate})::text j FROM filter_json`;
          const expected = await outcome(sql, true);
          expect(await outcome(sql, false), sql).toEqual(expected);
          reach.reach(keep === true ? "kept error" : keep === false ? "false mask" : "NULL mask");
          reach.reach(
            shape < 2 ? "value expression" : shape < 4 ? "key expression" : "ordering expression",
          );
          if (filterFault && keep === true && shape < 2 && bad !== "bad")
            reach.reach("filter error before value error");
          reach.reach(shape % 2 ? "JSONB" : "JSON");
          reach.reach("mixed accepted and rejected rows");
          reach.reach("PG rows or SQLSTATE");
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [0, false, 3, 1, "bad", false],
          [1, null, 3, 1, "bad", false],
          [2, false, 3, 1, "bad", false],
          [3, null, 3, 1, "bad", false],
          [4, false, 3, 1, "bad", false],
          [5, null, 3, 1, "bad", false],
          [0, true, 3, 1, "bad", false],
          [0, true, 3, 1, '["\\u0000"]', true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
