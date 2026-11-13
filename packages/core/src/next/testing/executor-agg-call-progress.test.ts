/** Each external helper must fit its actual step and its constructed slot, under pressure. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, AGG_CALL_PROGRESS_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { StepLoans } from "../memory/step-loans.js";
import { QueryRuntime } from "../exec/run.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- applied with original receiver
const reserve = QueryRuntime.prototype.reserve;
// eslint-disable-next-line @typescript-eslint/unbound-method -- applied with original receiver
const borrow = StepLoans.prototype.borrow;
// eslint-disable-next-line @typescript-eslint/unbound-method -- applied with original receiver
const restorePhase = QueryRuntime.prototype.restorePhase;
const shapes = [
  ["max(a) v", "max"],
  ["min(a) v", "min"],
  ["min(DISTINCT 'k'::text) k,max(a) v", "omitted extrema DISTINCT"],
  ["min(DISTINCT a) k,max(a) v", "wide extrema DISTINCT"],
  ["count(DISTINCT i) n,max(a) v", "preceding DISTINCT"],
  ["max(a) v,count(DISTINCT i) n", "following DISTINCT"],
  ["count(DISTINCT a) n,max(a) v", "wide DISTINCT"],
] as const;
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("next-agg-call-progress", [0xb7a3, 0xb7a4]))(
  "funds constructed variable-transition and DISTINCT helper steps seed%i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach("external call progress", AGG_CALL_PROGRESS_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: shapes.length - 1 }),
          fc.integer({ min: 580000, max: 810000 }),
          fc.boolean(),
          fc.boolean(),
          async (shape, width, unicode, cancel) => {
            reach.beginHistory();
            const governor = new Governor(128 << 20);
            let competitor = await governor.admit({
              label: "pinned competing holder",
              minimumBytes: 0,
              spillable: true,
            });
            const abort = new AbortController();
            const observed = { pinned: false, loans: 0, callLoans: 0, cancelled: false };
            const mocks: Array<{ mockRestore(): void }> = [];
            try {
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
                  this: QueryRuntime,
                  ...args
                ) {
                  await reserve.apply(this, args);
                  if (this.request.governor === governor && !observed.pinned) {
                    expect(
                      competitor.allocate(
                        governor.capBytes - governor.reserveBytes - governor.usedBytes,
                      ),
                    ).toBe(true);
                    observed.pinned = true;
                  }
                }),
              );
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "restorePhase").mockImplementation(async function (
                  this: QueryRuntime,
                  ...args
                ) {
                  if (this.request.governor !== governor) return restorePhase.apply(this, args);
                  // A finite competitor releases for the next atomic phase, then pins
                  // ordinary headroom again. Transition steps must still use their lane.
                  competitor.close();
                  await restorePhase.apply(this, args);
                  competitor = await governor.admit({
                    label: "repinned competitor",
                    minimumBytes: 0,
                    spillable: true,
                  });
                  expect(
                    competitor.allocate(
                      governor.capBytes - governor.reserveBytes - governor.usedBytes,
                    ),
                  ).toBe(true);
                }),
              );
              mocks.push(
                vi.spyOn(StepLoans.prototype, "borrow").mockImplementation(function (
                  this: StepLoans,
                  bytes,
                  signal,
                ) {
                  if (this.label.includes("/aggregate#")) {
                    observed.loans++;
                    const slot = /\/loan\/(\d+)\/progress$/.exec(this.label)?.[1];
                    if (slot !== undefined && Number(slot) >= 2) observed.callLoans++;
                    expect(bytes).toBeLessThanOrEqual(this.capacity);
                    if (cancel && !observed.cancelled) {
                      observed.cancelled = true;
                      queueMicrotask(() => abort.abort());
                    }
                  }
                  return borrow.call(this, bytes, signal);
                }),
              );
              const prepare = core.executor.prepare.bind(core.executor);
              mocks.push(
                vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                  const prepared = prepare(plan);
                  return {
                    ...prepared,
                    execute: (request) =>
                      prepared.execute({
                        ...request,
                        governor,
                        signal: AbortSignal.any([abort.signal, AbortSignal.timeout(60000)]),
                      }),
                  };
                }),
              );
              const selected = shapes[shape];
              if (!selected) throw new Error("missing aggregate shape");
              const [calls, name] = selected;
              const sql = `SELECT ${calls} FROM (SELECT $1::text a,i FROM generate_series(1,4)g(i))q`;
              const args = [unicode ? "汉".repeat(Math.floor(width / 3)) : "x".repeat(width)];
              if (cancel) {
                await expect(core.execute(sql, args)).rejects.toMatchObject({ sqlState: "57014" });
                expect(observed.cancelled).toBe(true);
                reach.reach("cancelled");
              } else {
                const result = await core.execute(sql, args);
                if (result.kind !== "rows" || !("batches" in result))
                  throw new Error("missing aggregate rows");
                expect(oracleRows(result)).toEqual((await pg.query(sql, args)).rows);
                reach.reach(name);
                reach.reach("PG exact");
              }
              expect(observed.pinned).toBe(true);
              expect(observed.loans).toBeGreaterThan(0);
              reach.reach("pinned competitor");
              if (observed.callLoans > 0) reach.reach("transition loan");
              reach.reach(unicode ? "UTF8" : "ASCII");
            } finally {
              for (const mock of mocks.reverse()) mock.mockRestore();
              competitor.close();
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
            }
          },
        ),
        {
          seed,
          numRuns: 12,
          endOnFailure: true,
          examples: [
            [0, 800000, false, false],
            [1, 800000, true, false],
            [2, 600000, false, false],
            [3, 800000, false, false],
            [4, 800000, true, false],
            [5, 800000, false, false],
            [6, 800000, false, false],
            [0, 800000, false, true],
            [2, 600000, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      vi.restoreAllMocks();
      await core.close();
      await pg.close();
    }
  },
  180000,
);
