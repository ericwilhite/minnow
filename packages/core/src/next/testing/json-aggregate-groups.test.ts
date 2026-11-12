/** Many groups and nonuniform members must spill under an ordinary small admission. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_AGGREGATE_GROUP_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { SortOperator } from "../exec/sort.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("CREATE TABLE jgroups(n INTEGER,g INTEGER,v TEXT)");
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("next-json-aggregate-groups", [0xb710, 0xb711]))(
  "spills many ordered groups and returns every exact member (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON many groups", JSON_AGGREGATE_GROUP_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(16, 32),
        fc.integer({ min: 12000, max: 13000 }),
        async (groups, width) => {
          reach.beginHistory();
          const core = await new StorageCore(16).open(),
            governor = new Governor(24 << 20),
            suffix = "x".repeat(width);
          await core.execute("CREATE TABLE jgroups(n INTEGER,g INTEGER,v TEXT)");
          const rows = Array.from({ length: 1024 }, (_, n) => ({
            n,
            g: n % groups,
            v: String(n) + ":" + suffix,
          }));
          await core.writeBatch("insertBatch", "jgroups", rows);
          await core.harness.path.recycleLog();
          await core.harness.path.quiesce();
          await pg.exec("TRUNCATE jgroups");
          await pg.query(
            "INSERT INTO jgroups SELECT n,n%$1::int,n::text||':'||$2::text FROM generate_series(0,1023)t(n)",
            [groups, suffix],
          );
          const sql =
              "SELECT g,json_agg(v ORDER BY n DESC)::text j,COUNT(*)::int n FROM jgroups GROUP BY g ORDER BY g",
            expected = (await pg.query(sql)).rows;
          const prepare = core.executor.prepare.bind(core.executor);
          const preparation = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
            const p = prepare(plan);
            return { ...p, execute: (r) => p.execute({ ...r, governor }) };
          });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- receiver supplied below
          const finish = SortOperator.prototype.finish;
          let nativeSpilled = 0;
          const sorts = vi
            .spyOn(SortOperator.prototype, "finish")
            .mockImplementation(async function (this: SortOperator) {
              try {
                await finish.call(this);
              } finally {
                nativeSpilled += this.profile.spilledBytes;
              }
            });
          try {
            const result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("Expected grouped rows");
            expect(oracleRows(result)).toEqual(expected);
            expect(expected.length).toBe(groups);
            expect(
              nativeSpilled,
              "observe shared sort spill, not just JSON's output checkpoint",
            ).toBeGreaterThan(0);
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach(groups === 16 ? "sixteen groups" : "thirty-two groups");
            reach.reach("observed native sort spill");
            reach.reach("ordered nonuniform members");
            reach.reach("ordinary sibling");
            reach.reach("exact PG text");
            reach.reach("zero resources");
          } finally {
            sorts.mockRestore();
            preparation.mockRestore();
            await core.close();
          }
        },
      ),
      {
        seed,
        numRuns: 2,
        examples: [
          [16, 12000],
          [32, 12000],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
