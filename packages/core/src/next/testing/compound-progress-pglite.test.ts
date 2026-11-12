/** PG bag/join semantics and governor rule 5: one complete step, never a nested loan. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { createReach, COMPOUND_PROGRESS_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { QueryRuntime } from "../exec/run.js";
import { Governor } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import { PressureCampaign, reserveWorkingSet } from "./compound-progress-history.test.fixture.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const canonical = (rows: readonly unknown[]) => rows.map((row) => JSON.stringify(row)).sort();

it.each(seedsFor("next-compound-progress", [0xb720, 0xb721]))(
  "preflights wide set/join steps with all ordinary credit occupied (seed %i)",
  async (seed) => {
    const reach = createReach("compound progress", COMPOUND_PROGRESS_REACH);
    const random = mulberry32(seed);
    const campaign = new PressureCampaign();
    const history = await campaign.history(64 << 20),
      { core } = history;
    try {
      const width = (5 << 20) + Math.floor(random() * 32768);
      const largeValue = "é".repeat(Math.floor(width / 2)) + "x";
      // Keep the original >5 MiB EXCEPT/INNER/large-source regressions. Other shapes
      // still force multi-copy loans with ~0.8 MiB UTF8 values, without repeating
      // >5 MiB scans eighteen times under coverage. No states/seeds are removed.
      const value = "é".repeat(400000 + (width & 16383)) + "x";
      // Keep SQL below the front end's one-Mi-character limit while exceeding
      // the blob floor's compound-copy allowance in UTF8 bytes.
      const literalValue = "漢".repeat(800000 + Math.floor(random() * 16000));
      const l = "(SELECT i AS n, $1::text AS j FROM generate_series(1,2) g(i)) l";
      const r = "(SELECT i AS n, $1::text AS j FROM generate_series(1,3,2) g(i)) r";
      await core.execute("CREATE TABLE held(n INTEGER,j TEXT)");
      await core.writeBatch("insertBatch", "held", [{ n: 1, j: largeValue }]);
      const cases: Array<[string, string]> = [
        ["EXCEPT", "SELECT $1::text AS j EXCEPT SELECT NULL::text AS j"],
        ["EXCEPT literal", `SELECT '${literalValue}'::text AS j EXCEPT SELECT NULL::text AS j`],
        ["EXCEPT stored", "SELECT j FROM held EXCEPT SELECT NULL::text AS j"],
        [
          "EXCEPT repeated parameter",
          "SELECT a=b AS same FROM (SELECT $1::text a,$1::text b EXCEPT SELECT NULL::text a,NULL::text b) q",
        ],
        [
          "EXCEPT ALL",
          "SELECT $1::text AS j FROM generate_series(1,2) g(i) EXCEPT ALL SELECT $1::text AS j",
        ],
        ["duplicates", `SELECT j FROM ${l} EXCEPT SELECT $1::text AS j`],
        ...(["INNER", "LEFT", "RIGHT", "FULL"] as const).map(
          (kind) =>
            [
              "join " + kind,
              `SELECT l.n, l.j, r.n AS rn FROM ${l} ${kind} JOIN ${r} ON l.n=r.n AND l.j=r.j`,
            ] as [string, string],
        ),
        [
          "join RIGHT empty probe",
          `SELECT r.n,r.j FROM (SELECT 1 AS n, $1::text AS j WHERE FALSE) l RIGHT JOIN ${r} ON l.n=r.n AND l.j=r.j`,
        ],
        [
          "join residual payload",
          `SELECT l.n,r.j,r.n AS rn FROM ${l} INNER JOIN ${r} ON l.n=r.n AND l.j<=r.j`,
        ],
        [
          "join RIGHT empty payload",
          `SELECT r.n,r.j FROM (SELECT 1 AS n, $1::text AS j WHERE FALSE) l RIGHT JOIN ${r} ON l.n=r.n`,
        ],
        ["join stored", "SELECT l.n,r.j FROM held l JOIN held r ON l.n=r.n"],
        [
          "join repeated parameter",
          "SELECT l.n,l.a=l.b AS same,r.n AS rn FROM (SELECT i AS n,$1::text AS a,$1::text AS b FROM generate_series(1,2) g(i)) l JOIN (SELECT i AS n,$1::text AS a,$1::text AS b FROM generate_series(1,3,2) g(i)) r ON l.n=r.n AND l.a=r.a AND l.b=r.b",
        ],
        [
          "semi",
          `SELECT l.n,l.j FROM ${l} WHERE EXISTS (SELECT 1 FROM ${r} WHERE l.n=r.n AND l.j=r.j)`,
        ],
        [
          "anti",
          `SELECT l.n,l.j FROM ${l} WHERE NOT EXISTS (SELECT 1 FROM ${r} WHERE l.n=r.n AND l.j=r.j)`,
        ],
        ["mark", `SELECT l.n,l.j,l.j IN (SELECT r.j FROM ${r}) AS m FROM ${l}`],
      ];
      for (const [state, sql] of cases) {
        history.guard();
        reach.beginHistory();
        const parameters =
          state === "EXCEPT literal" || state.endsWith("stored")
            ? []
            : [state === "EXCEPT" || state === "join INNER" ? largeValue : value];
        if (state === "EXCEPT stored") {
          await pg.exec("DROP TABLE IF EXISTS held; CREATE TEMP TABLE held(n INTEGER,j TEXT)");
          await pg.query("INSERT INTO held VALUES (1,$1::text)", [largeValue]);
        }
        if (state === "join INNER") {
          expect(largeValue.length * 2 - 1).toBeGreaterThan(5 << 20);
          reach.reach("large recursive join");
        }
        const expected = await pg.query(sql, parameters);
        history.guard();
        const governor = new Governor(
          state.endsWith("stored") ? 128 << 20 : state.startsWith("EXCEPT") ? 64 << 20 : 512 << 20,
        );
        const blocker = await governor.admit({
          label: "competing retained grant",
          minimumBytes: 0,
          spillable: true,
        });
        history.guard();
        const closeBlocker = history.ownCleanup(() => blocker.close());
        let blocked = false;
        const closeHook = history.observe(() =>
          vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
            this: QueryRuntime,
            ...args
          ) {
            await reserveWorkingSet.apply(this, args);
            if (this.request.governor === governor && !blocked) {
              expect(
                blocker.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
              ).toBe(true);
              blocked = true;
            }
          }),
        );
        const prepare = core.executor.prepare.bind(core.executor);
        const closeAdapter = history.observe(() =>
          vi
            .spyOn(core.executor, "prepare")
            .mockImplementation((plan) => history.wrap(prepare(plan), governor)),
        );
        try {
          const result = await core.execute(sql, parameters);
          if (result.kind !== "rows" || !("batches" in result))
            throw new Error("Expected SQL rows");
          expect(canonical(oracleRows(result))).toEqual(canonical(expected.rows));
          expect(blocked).toBe(true);
          expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          reach.reach(state as (typeof COMPOUND_PROGRESS_REACH)[number]);
          reach.reach("PG exact");
          reach.reach("all ordinary credit occupied");
        } catch (error) {
          throw new Error(state, { cause: error });
        } finally {
          closeHook();
          closeAdapter();
          await closeBlocker();
          expect(governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("cleanup");
        }
      }
      reach.assertReached();
    } finally {
      await campaign.close();
    }
  },
  120000,
);

it("audits the other set operations without adding an execution path", async () => {
  const core = await new StorageCore().open();
  try {
    const sql = "SELECT 1 AS n UNION ALL SELECT 1 AS n UNION ALL SELECT NULL::int AS n";
    const result = await core.execute(sql);
    if (result.kind !== "rows" || !("batches" in result))
      throw new Error("Expected literal UNION ALL rows");
    expect(canonical(oracleRows(result))).toEqual(canonical((await pg.query(sql)).rows));
    for (const operator of ["INTERSECT", "INTERSECT ALL", "UNION"]) {
      const query = `SELECT 1 AS n ${operator} SELECT 1 AS n`;
      expect((await pg.query(query)).rows).toEqual([{ n: 1 }]);
      await expect(core.execute(query)).rejects.toMatchObject({ sqlState: "0A000" });
    }
  } finally {
    await core.close();
  }
});
