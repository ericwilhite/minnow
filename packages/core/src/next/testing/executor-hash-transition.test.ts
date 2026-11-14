/** Hash key steps must progress under ordinary, pinned and cancellable reclaim pressure. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, HASH_TRANSITION_REACH } from "../../testing/reach.js";
import { AggregateOperator } from "../exec/agg/aggregate.js";
import { Governor } from "../memory/governor.js";
import { QueryRuntime } from "../exec/run.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver
const reserve = QueryRuntime.prototype.reserve;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver
const restore = QueryRuntime.prototype.restorePhase;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver
const pushPrepared = AggregateOperator.prototype.pushPrepared;
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
          fc.integer({ min: 1, max: 16 }),
          fc.integer({ min: 0, max: 3 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.constantFrom(16, 32, 64, 128, 256),
          async (width, count, mode, distinct, unicode, prefill, mixed, cap) => {
            reach.beginHistory();
            const value = unicode ? "汉".repeat(Math.floor(width / 3)) : "x".repeat(width);
            const args = Array.from({ length: distinct ? count : 1 }, (_, c) => value + String(c));
            const keys = Array.from({ length: count }, (_, c) => `k${String(c)}`);
            const expressions = keys.map(
              (k, c) =>
                `CASE WHEN i=${prefill ? "2049" : "1"} THEN $${String(distinct ? c + 1 : 1)}::text ${prefill ? "WHEN i=2050 THEN 'k' ELSE 'p'" : "ELSE 'k'"} END ${k}`,
            );
            if (mixed) {
              keys.unshift("j");
              expressions.unshift("CASE WHEN i<=2048 OR i>=2051 THEN 0 ELSE 2147483647-i END j");
            }
            const sql = `SELECT count(*) n,sum(v) s,avg(v) a FROM (SELECT ${expressions.join(",")},CASE WHEN i%3=0 THEN 1e16::float8 WHEN i%3=1 THEN 1::float8 ELSE -1e16::float8 END v FROM generate_series(1,${prefill ? "2052" : "3"})g(i))q GROUP BY ${keys.join(",")}`;
            const governor = new Governor((distinct ? Math.max(128, cap) : cap) << 20);
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
              if (prefill && mode === 0)
                mocks.push(
                  vi
                    .spyOn(AggregateOperator.prototype, "pushPrepared")
                    .mockImplementation(function (this: AggregateOperator, ...a) {
                      const result = pushPrepared.apply(this, a);
                      // Pin only AFTER a real prefix transitioned. This forces exact saved
                      // float state instead of depending on host-dependent batch geometry.
                      if (!pinned) pin();
                      return result;
                    }),
                );
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
                const actualCap = governor.capBytes / (1 << 20);
                if (actualCap === 16) reach.reach("16 MiB");
                if (actualCap === 32) reach.reach("32 MiB");
                if (actualCap === 64) reach.reach("64 MiB");
                if (actualCap === 128) reach.reach("128 MiB");
                if (count === 12) reach.reach("12 keys");
                if (count === 16) reach.reach("16 keys");
                if (mixed) reach.reach("mixed fixed/byte keys");
                if (prefill) {
                  reach.reach("resident prefix");
                  if (mode === 0) {
                    expect(phases).toBeGreaterThan(0);
                    reach.reach("saved float prefix");
                  }
                }
                if (mode === 0 && !prefill) reach.reach("ordinary");
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
          numRuns: 23, // Preserve ten old cases, add five cap/key witnesses, plus eight random cases.
          endOnFailure: true,
          examples: [
            [800000, 1, 0, false, false, false, false, 256],
            [800000, 1, 1, false, false, false, false, 256],
            [800000, 3, 2, true, false, false, false, 256],
            [800000, 1, 3, false, false, false, false, 256],
            [800000, 16, 0, false, false, false, false, 256],
            [800000, 8, 1, true, true, false, false, 256],
            [800000, 1, 0, false, false, true, false, 256],
            [600000, 3, 1, true, false, true, false, 256],
            [800000, 1, 0, false, false, true, true, 256],
            [600000, 3, 1, true, true, true, true, 256],
            [800000, 12, 0, false, false, false, false, 16],
            [800000, 12, 1, false, false, false, false, 32],
            [800000, 16, 0, false, false, false, false, 64],
            [800000, 16, 1, false, false, false, false, 128],
            [800000, 16, 0, false, false, false, false, 16],
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
