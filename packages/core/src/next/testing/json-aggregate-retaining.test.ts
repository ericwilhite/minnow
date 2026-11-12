/** JSON aggregate cuts restore native retaining parents without replaying consumed inputs. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_AGGREGATE_PARENT_REACH } from "../../testing/reach.js";
import { HashJoin } from "../exec/join.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec(
    "CREATE TABLE jp(n INTEGER,k INTEGER,v TEXT);CREATE TABLE ju(k INTEGER,label TEXT)",
  );
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("next-json-aggregate-parents", [0xb714, 0xb715]))(
  "keeps joins, windows, bindings and NULLs exact across cuts (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "JSON aggregate retaining parents",
      JSON_AGGREGATE_PARENT_REACH,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 3, max: 12 }),
        fc.boolean(),
        fc.boolean(),
        async (length, binary, desc) => {
          reach.beginHistory();
          const core = await new StorageCore().open(),
            t = Array.from({ length }, (_, n) => ({
              n,
              k: n % 3,
              v: n % 4 === 0 ? null : String(n) + '汉"\\',
            })),
            u = [
              { k: 0, label: "a" },
              { k: 0, label: "b" },
              { k: 2, label: "c" },
              { k: 4, label: "d" },
              { k: null, label: "e" },
            ];
          await core.execute("CREATE TABLE jp(n INTEGER,k INTEGER,v TEXT)");
          await core.execute("CREATE TABLE ju(k INTEGER,label TEXT)");
          await core.writeBatch("insertBatch", "jp", t);
          await core.writeBatch("insertBatch", "ju", u);
          await pg.exec("TRUNCATE jp,ju");
          for (const r of t) await pg.query("INSERT INTO jp VALUES($1,$2,$3)", [r.n, r.k, r.v]);
          for (const r of u) await pg.query("INSERT INTO ju VALUES($1,$2)", [r.k, r.label]);
          // eslint-disable-next-line @typescript-eslint/unbound-method -- same receiver below
          const checkpoint = HashJoin.prototype.checkpoint;
          let built = false;
          const spy = vi.spyOn(HashJoin.prototype, "checkpoint").mockImplementation(async function (
            this: HashJoin,
          ) {
            const saved = await checkpoint.call(this);
            built ||= saved.buildComplete;
            return saved;
          });
          const base = `SELECT k,${binary ? "jsonb_agg" : "json_agg"}(v ORDER BY n ${desc ? "DESC" : "ASC"})::text j FROM jp WHERE $1::bool GROUP BY k`;
          try {
            for (const present of [true, false])
              for (const shape of ["join", "window"] as const) {
                const sql =
                  shape === "join"
                    ? `SELECT a.k,a.j,u.label FROM(${base})a LEFT JOIN ju u ON u.k=a.k ORDER BY a.k,u.label`
                    : `SELECT k,j,rn::int rn FROM(SELECT k,j,ROW_NUMBER()OVER(ORDER BY k)rn FROM(${base})a)s ORDER BY k`;
                const expected = (await pg.query(sql, [present])).rows,
                  result = await core.execute(sql, [present]);
                if (!("batches" in result)) throw new Error("Expected retained JSON rows");
                expect(oracleRows(result), sql).toEqual(expected);
                reach.reach(shape === "join" ? "retaining join" : "retaining window");
                reach.reach(present ? "bound true" : "bound false");
              }
            expect(built, "the join's consumed build arm must be checkpointed, not replayed").toBe(
              true,
            );
            reach.reach("consumed build arm");
            reach.reach(binary ? "JSONB" : "JSON");
            reach.reach("exact text");
          } finally {
            spy.mockRestore();
            await core.close();
          }
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("zero files");
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [6, false, false],
          [6, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
