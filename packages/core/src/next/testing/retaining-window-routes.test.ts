/** A potential phase cut must not demote plentiful-memory SQL windows. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, RETAINING_WINDOW_ROUTE_REACH } from "../../testing/reach.js";
import { WindowOperator } from "../exec/window.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
const reach = createReach("retaining window routes", RETAINING_WINDOW_ROUTE_REACH);
let core: StorageCore, pg: PGlite;
beforeAll(async () => {
  core = await new StorageCore().open();
  pg = new PGlite();
  await pg.waitReady;
  const ddl = "CREATE TABLE t(k INTEGER,p INTEGER,v DOUBLE PRECISION,j TEXT)";
  await core.execute(ddl);
  await pg.exec(ddl);
  const rows = Array.from({ length: 128 }, (_, i) => ({
    k: i,
    p: i % 3,
    v: i % 9,
    j: '{"a":1}',
  }));
  await core.writeBatch("insertBatch", "t", rows);
  for (const r of rows) await pg.query("INSERT INTO t VALUES($1,$2,$3,$4)", [r.k, r.p, r.v, r.j]);
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
  reach.assertReached();
});
for (const seed of seedsFor("next-retaining-window-routes", [0xb6c0, 0xb6c1]))
  it(`retains grouped execution through potential aggregate/JSON cuts (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("row_number", "rank", "sum", "lag", "avg"),
        fc.boolean(),
        async (fn, json) => {
          reach.beginHistory();
          const baseline = core.harness.governor.usedBytes;
          const source = json
            ? "SELECT k,p,v,j::jsonb j FROM t"
            : "SELECT k,p,sum(v) v FROM t GROUP BY p,k";
          const expr =
            fn === "sum" || fn === "avg" ? `${fn}(v)` : fn === "lag" ? "lag(v)" : `${fn}()`;
          const sql = `SELECT k,p,${json ? "j::text," : ""}${expr} OVER(PARTITION BY ${json ? "j::text" : "p"} ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) w FROM (${source}) a ORDER BY p,k,v`;
          const profiles: Array<{
            checkpointable: boolean;
            groupedRows: number;
            spilledBytes: number;
          }> = [];
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked on the same window
          const finish = WindowOperator.prototype.finish;
          const spy = vi.spyOn(WindowOperator.prototype, "finish").mockImplementation(function (
            this: WindowOperator,
          ) {
            return Promise.resolve(finish.call(this)).then(() => {
              profiles.push({
                checkpointable: this.spec.checkpointable === true,
                groupedRows: this.profile.groupedRows,
                spilledBytes: this.profile.sort.spilledBytes,
              });
            });
          });
          try {
            const actual = await core.execute(sql);
            expect(actual.kind).toBe("rows");
            if (actual.kind !== "rows" || !("columns" in actual)) throw new Error("missing rows");
            const expected = (await pg.query(sql)).rows;
            expect(oracleRows(actual)).toEqual(expected);
            expect(profiles.length).toBeGreaterThan(0);
            expect(
              profiles.every((p) => p.checkpointable && p.groupedRows > 0 && p.spilledBytes === 0),
              JSON.stringify(profiles),
            ).toBe(true);
            reach.reach(json ? "JSON producer" : "hybrid producer");
            reach.reach(fn);
          } finally {
            spy.mockRestore();
          }
          expect(core.harness.governor.usedBytes).toBe(baseline);
          expect(core.spill.bytesInUse).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 10,
        examples: [
          ["row_number", false],
          ["rank", true],
          ["sum", false],
          ["lag", true],
          ["avg", false],
        ],
      },
    );
  }, 60000);
