/** Hash key steps must progress under ordinary, pinned and cancellable reclaim pressure. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, HASH_TRANSITION_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { QueryRuntime } from "../exec/run.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver
const reserve = QueryRuntime.prototype.reserve;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver
const restore = QueryRuntime.prototype.restorePhase;
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("executor-hash-transition", [0xb7bf, 0xb7c0]))(
  "finishes byte transitions with bounded peaks and cancellable waits seed%i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach("hash transition SQL", HASH_TRANSITION_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 580000, max: 810000 }),
          fc.integer({ min: 1, max: 8 }),
          fc.integer({ min: 0, max: 3 }),
          fc.boolean(),
          fc.boolean(),
          async (width, count, mode, distinct, unicode) => {
            reach.beginHistory();
            const value = unicode ? "汉".repeat(Math.floor(width / 3)) : "x".repeat(width);
            const args = Array.from({ length: distinct ? count : 1 }, (_, c) => value + String(c));
            const keys = Array.from({ length: count }, (_, c) => `k${String(c)}`);
            const expressions = keys.map(
              (k, c) =>
                `CASE WHEN i=1 THEN $${String(distinct ? c + 1 : 1)}::text ELSE 'k' END ${k}`,
            );
            const sql = `SELECT count(*) n FROM (SELECT ${expressions.join(",")} FROM generate_series(1,3)g(i))q GROUP BY ${keys.join(",")}`;
            const governor = new Governor(256 << 20);
            let competitor = await governor.admit({
              label: "competitor",
              minimumBytes: 0,
              spillable: true,
            });
            const signal = new AbortController();
            const reclaims: Array<Promise<void>> = [];
            let pinned = false,
              asked = false,
              phases = 0;
            const prepare = core.executor.prepare.bind(core.executor);
            const mocks: Array<{ mockRestore(): void }> = [];
            const pin = () => {
              expect(
                competitor.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
              ).toBe(true);
              pinned = true;
              if (mode >= 2)
                competitor.onReclaimRequest(() => {
                  asked = true;
                  if (mode === 3) signal.abort(new Error("cancel hash reclaim"));
                  const holder = competitor;
                  const work = new Promise<void>((resolve) => setTimeout(resolve, 10)).then(() =>
                    holder.close(),
                  );
                  reclaims.push(work);
                  return work;
                });
            };
            try {
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
                  this: QueryRuntime,
                  ...a
                ) {
                  await reserve.apply(this, a);
                  if (this.request.governor === governor && mode !== 0 && !pinned) pin();
                }),
              );
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "restorePhase").mockImplementation(async function (
                  this: QueryRuntime,
                  ...a
                ) {
                  if (this.request.governor !== governor) return restore.apply(this, a);
                  phases++;
                  competitor.close();
                  await restore.apply(this, a);
                  competitor = await governor.admit({
                    label: "repin",
                    minimumBytes: 0,
                    spillable: true,
                  });
                  if (mode !== 0) pin();
                }),
              );
              mocks.push(
                vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                  const q = prepare(plan);
                  return {
                    ...q,
                    execute: (request) =>
                      q.execute({
                        ...request,
                        governor,
                        signal: AbortSignal.any([signal.signal, AbortSignal.timeout(60000)]),
                      }),
                  };
                }),
              );
              if (mode === 3) {
                await expect(core.execute(sql, args)).rejects.toMatchObject({ sqlState: "57014" });
                expect(asked).toBe(true);
                reach.reach("cancel reclaim");
              } else {
                const got = await core.execute(sql, args);
                if (got.kind !== "rows" || !("batches" in got)) throw new Error("missing rows");
                const canonical = (rows: ReadonlyArray<Record<string, unknown>>) =>
                  rows.map((r) => JSON.stringify(r)).sort();
                expect(canonical(oracleRows(got))).toEqual(
                  canonical((await pg.query<Record<string, unknown>>(sql, args)).rows),
                );
                reach.reach("PG exact");
                if (mode === 0) reach.reach("ordinary");
                if (mode === 1) {
                  expect(phases).toBeGreaterThan(0);
                  expect(core.lastProfile?.operators.some((o) => o.spilledBytes > 0)).toBe(true);
                  reach.reach("pinned spill");
                }
                if (mode === 2) {
                  expect(asked).toBe(true);
                  reach.reach("reclaim release");
                }
              }
              reach.reach(distinct ? "distinct parameters" : "repeated parameters");
              if (count > 1) reach.reach("compound keys");
              if (unicode) reach.reach("UTF8");
            } finally {
              for (const m of mocks.reverse()) m.mockRestore();
              await Promise.allSettled(reclaims);
              competitor.close();
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("zero");
            }
          },
        ),
        {
          seed,
          numRuns: 8,
          endOnFailure: true,
          examples: [
            [800000, 1, 0, false, false],
            [800000, 1, 1, false, false],
            [800000, 3, 2, true, false],
            [800000, 1, 3, false, false],
            [800000, 16, 0, false, false],
            [800000, 8, 1, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  },
  120000,
);
