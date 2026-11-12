/** Repeated join predicates need the same compound credit through SQL and stored reads. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { createReach, JOIN_REPEATED_SQL_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import type { Rel } from "../ir/ir.js";
import { QueryRuntime } from "../exec/run.js";
import { Governor } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { PressureCampaign, reserveWorkingSet } from "./compound-progress-history.test.fixture.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const canonical = (rows: readonly unknown[]) => rows.map((r) => JSON.stringify(r)).sort();
it.each(seedsFor("next-join-repeated-sql", [0xb736, 0xb737]))(
  "preflights repeated SQL keys with competing grants (seed %i)",
  async (seed) => {
    const reach = createReach("repeated SQL join keys", JOIN_REPEATED_SQL_REACH),
      random = mulberry32(seed),
      campaign = new PressureCampaign();
    const history = await campaign.history(256 << 20),
      { core } = history;
    const text = "x".repeat(seed % 2 ? 800000 : 800000 + Math.floor(random() * 8000));
    const storedValue = text.slice(0, Math.floor(text.length / 2));
    try {
      // INNER retains every large planner/source witness; the other join kinds
      // use 16KiB counterparts. Native pressure anchors still cover all seven kinds.
      for (const size of ["wide", "tiny"] as const) {
        const value = size === "wide" ? storedValue : text.slice(0, 16384);
        for (const one of [false, true])
          for (const side of ["l", "r"]) {
            const table = `repeat_${size}_${one ? "one_" : ""}${side}`;
            await core.execute(`CREATE TABLE ${table}(n INTEGER,a TEXT,b TEXT,c TEXT)`);
            await pg.exec(
              `DROP TABLE IF EXISTS ${table};CREATE TEMP TABLE ${table}(n INTEGER,a TEXT,b TEXT,c TEXT)`,
            );
            const ns = one ? [1] : side === "l" ? [1, 2] : [1, 3];
            await core.writeBatch(
              "insertBatch",
              table,
              ns.map((n) => ({ n, a: value, b: value, c: value })),
            );
            for (const n of ns)
              await pg.query(`INSERT INTO ${table} VALUES($1,$2,$2,$2)`, [n, value]);
          }
      }
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      for (const grid of [false, true])
        for (const source of ["parameter", "stored", "single"] as const)
          for (const kind of ["inner", "left", "right", "full"] as const) {
            history.guard();
            reach.beginHistory();
            const stored = source !== "parameter",
              wide = kind === "inner";
            const prefix = `repeat_${wide ? "wide" : "tiny"}_${source === "single" ? "one_" : ""}`;
            const l = stored
              ? `${prefix}l l`
              : "(SELECT i n,$1::text a,$1::text b,$1::text c FROM generate_series(1,2)g(i))l";
            const r = stored
              ? `${prefix}r r`
              : "(SELECT i n,$1::text a,$1::text b,$1::text c FROM generate_series(1,3,2)g(i))r";
            const equal = grid
              ? "l.n=r.n AND " +
                ["a", "b", "c"]
                  .flatMap((a) => ["a", "b", "c"].map((b) => `l.${a}=r.${b}`))
                  .join(" AND ")
              : "l.n=r.n AND l.a=r.a AND l.a=r.b AND l.b=r.b";
            const sql = `SELECT l.n,r.n rn FROM ${l} ${kind.toUpperCase()} JOIN ${r} ON ${equal}`;
            if (grid) {
              const lowered = core.lowerer.lower(sql, core.context());
              if (lowered.kind !== "query") throw new Error("Expected lowered join");
              const find = (rel: Rel): Extract<Rel, { op: "join" }> | undefined => {
                if (rel.op === "join") return rel;
                return "input" in rel ? find(rel.input) : undefined;
              };
              expect(find(lowered.plan.root)?.leftKeys.length).toBe(10);
              reach.reach("cross-field grid");
            }
            const parameters = stored
                ? []
                : [wide ? (grid ? storedValue : text) : text.slice(0, 16384)],
              expected = await pg.query(sql, parameters),
              governor = new Governor(256 << 20);
            const blocker = await governor.admit({
              label: "competing ordinary SQL credit",
              minimumBytes: 0,
              spillable: true,
            });
            const closeBlocker = history.ownCleanup(() => blocker.close());
            let occupied = false;
            const closeHook = history.observe(() =>
              vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
                this: QueryRuntime,
                ...args
              ) {
                await reserveWorkingSet.apply(this, args);
                if (this.request.governor === governor && !occupied) {
                  expect(
                    blocker.allocate(
                      governor.capBytes - governor.reserveBytes - governor.usedBytes,
                    ),
                  ).toBe(true);
                  occupied = true;
                }
              }),
            );
            const prepare = core.executor.prepare.bind(core.executor),
              closeAdapter = history.observe(() =>
                vi
                  .spyOn(core.executor, "prepare")
                  .mockImplementation((plan) => history.wrap(prepare(plan), governor)),
              );
            try {
              const actual = await core.execute(sql, parameters);
              if (!("batches" in actual)) throw new Error("Expected rows");
              expect(
                canonical(oracleRows(actual)),
                `${kind}/${source}/${grid ? "grid" : "asymmetric"}`,
              ).toEqual(canonical(expected.rows));
              expect(occupied).toBe(true);
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              reach.reach(kind);
              reach.reach(stored ? "stored source" : "repeated parameter");
              if (source === "single") reach.reach("single-row source envelope");
              reach.reach("asymmetric repeated predicates");
              reach.reach("ordinary credit occupied");
              reach.reach("PG exact");
            } catch (error) {
              throw new Error(`${kind}/${stored ? "stored" : "parameter"}`, { cause: error });
            } finally {
              closeHook();
              closeAdapter();
              await closeBlocker();
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
            }
            reach.reach("zero resources");
          }
      reach.assertReached();
    } finally {
      await campaign.close();
    }
  },
  120000,
);
