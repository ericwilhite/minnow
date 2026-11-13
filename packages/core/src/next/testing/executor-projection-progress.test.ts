/** Reachable projection outputs must progress under ordinary competing pressure. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PROJECTION_PROGRESS_REACH } from "../../testing/reach.js";
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
  ["CASE WHEN i=1 THEN $1::text ELSE 'k'::text END", "CASE"],
  ["coalesce(NULL::text, nullif($1::text,'absent'::text), 'k'::text)", "nested COALESCE NULLIF"],
  ["CASE WHEN i=1 THEN a ELSE 'k'::text END", "column CASE"],
  ["coalesce('k'::text, $1::text)", "short reachable branch"],
] as const;
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("next-projection-progress", [0xb7b1, 0xb7b2]))(
  "projects reachable wide branches under competing pressure seed%i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach("projection progress", PROJECTION_PROGRESS_REACH);
    try {
      await pg.waitReady;
      await core.execute("CREATE TABLE projection_values(a TEXT,i INTEGER,h INTEGER)");
      await pg.exec("CREATE TABLE projection_values(a TEXT,i INTEGER,h INTEGER)");
      let history = 0;
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: shapes.length - 1 }),
          fc.integer({ min: 580000, max: 810000 }),
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 0, max: 2 }),
          fc.boolean(),
          async (shape, width, unicode, cancel, input, direct) => {
            reach.beginHistory();
            history++;
            const value =
              input === 1
                ? null
                : input === 2
                  ? "absent"
                  : unicode
                    ? "汉".repeat(Math.floor(width / 3))
                    : "x".repeat(width);
            if (shape === 2) {
              await core.writeBatch(
                "insertBatch",
                "projection_values",
                [1, 2].map((i) => ({ a: value, i, h: history })),
              );
              for (const i of [1, 2])
                await pg.query("INSERT INTO projection_values VALUES($1,$2,$3)", [
                  value,
                  i,
                  history,
                ]);
            }
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
                    if (cancel) {
                      observed.cancelled = true;
                      queueMicrotask(() => abort.abort());
                    }
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
              const [expression, name] = selected;
              const source =
                shape === 2
                  ? `(SELECT a,i FROM projection_values WHERE h=${String(history)})src`
                  : "(SELECT $1::text a,i FROM generate_series(1,2)g(i))src";
              const sql = direct
                ? `SELECT ${expression} k FROM ${source}`
                : `SELECT count(*) n FROM (SELECT ${expression} k FROM ${source})q GROUP BY k`;
              const args = shape === 2 ? [] : [value];
              if (cancel) {
                await expect(core.execute(sql, args)).rejects.toMatchObject({ sqlState: "57014" });
                expect(observed.cancelled).toBe(true);
                reach.reach("cancelled");
              } else {
                const result = await core.execute(sql, args);
                if (result.kind !== "rows" || !("batches" in result))
                  throw new Error("missing aggregate rows");
                const canonical = (rows: readonly unknown[]) =>
                  rows.map((row) => JSON.stringify(row)).sort();
                expect(canonical(oracleRows(result))).toEqual(
                  canonical((await pg.query(sql, args)).rows),
                );
                reach.reach(name);
                reach.reach("PG exact");
                if (direct && input === 0) reach.reach("wide result delivery");
                if (input === 1) reach.reach("NULL input");
                if (shape === 1 && input === 2) reach.reach("NULLIF equal");
              }
              expect(observed.pinned).toBe(true);

              reach.reach("pinned competitor");

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
          numRuns: 8,
          endOnFailure: true,
          examples: [
            [0, 800000, false, false, 0, false],
            [1, 800000, true, false, 0, false],
            [2, 800000, false, false, 0, false],
            [3, 800000, false, false, 0, false],
            [0, 800000, false, true, 0, false],
            [0, 800000, false, false, 1, false],
            [1, 800000, false, false, 2, false],
            [0, 800000, false, false, 0, true],
            [2, 800000, true, false, 0, true],
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
