/** PG grouping is unchanged when a row repeats an owned parameter in several positions. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, AGG_WIDE_PROGRESS_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { StepLoans } from "../memory/step-loans.js";
import { ColumnStore } from "../exec/column-store.js";
import { QueryRuntime } from "../exec/run.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- applied with original receiver
const reserve = QueryRuntime.prototype.reserve;
// eslint-disable-next-line @typescript-eslint/unbound-method -- applied with original receiver
const borrow = StepLoans.prototype.borrow;
// eslint-disable-next-line @typescript-eslint/unbound-method -- applied with original receiver
const appendNeed = ColumnStore.prototype.appendNeed;
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("next-agg-wide-progress", [0xb791, 0xb792]))(
  "wide aggregate sort under pinned ordinary credit seed%i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach("wide aggregate progress", AGG_WIDE_PROGRESS_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 4 }),
          fc.integer({ min: 780000, max: 800000 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (n, bytes, unicode, distinct, cancel) => {
            reach.beginHistory();
            const governor = new Governor(256 << 20),
              blocker = await governor.admit({
                label: "competing pinned holder",
                minimumBytes: 0,
                spillable: true,
              });
            let occupied = false,
              cancelled = false;
            const staging = { partial: false };
            const abort = new AbortController();
            const requests: Array<{ bytes: number; capacity: number }> = [];
            const mocks: Array<{ mockRestore(): void }> = [];
            try {
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
                  this: QueryRuntime,
                  ...args
                ) {
                  await reserve.apply(this, args);
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
              mocks.push(
                vi.spyOn(StepLoans.prototype, "borrow").mockImplementation(function (
                  this: StepLoans,
                  count,
                  signal,
                ) {
                  if (this.label.includes("/aggregate#")) {
                    requests.push({ bytes: count, capacity: this.capacity });
                    if (cancel && !cancelled) {
                      cancelled = true;
                      queueMicrotask(() => abort.abort());
                    }
                  }
                  return borrow.call(this, count, signal);
                }),
              );
              mocks.push(
                vi.spyOn(ColumnStore.prototype, "appendNeed").mockImplementation(function (
                  this: ColumnStore,
                  ...args
                ) {
                  if (this.rows === 0 && this.retainedBytes > 1024 * 1024) staging.partial = true;
                  return appendNeed.apply(this, args);
                }),
              );
              const prepared = core.executor.prepare.bind(core.executor);
              mocks.push(
                vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                  const p = prepared(plan);
                  return {
                    ...p,
                    execute: (request) =>
                      p.execute({
                        ...request,
                        governor,
                        signal: request.signal
                          ? AbortSignal.any([request.signal, abort.signal])
                          : abort.signal,
                      }),
                  };
                }),
              );
              const value = unicode ? "汉".repeat(Math.floor(bytes / 3)) : "x".repeat(bytes),
                args = distinct ? Array.from({ length: n }, (_, i) => value + String(i)) : [value];
              const fields = Array.from(
                { length: n },
                (_, i) => `$${String(distinct ? i + 1 : 1)}::text AS a${String(i)}`,
              );
              const group = Array.from({ length: n }, (_, i) => `a${String(i)}`);
              const sql = `SELECT count(*) AS n FROM (SELECT ${fields.join(",")} FROM generate_series(1,2) g(i)) q GROUP BY ${group.join(",")}`;
              if (cancel) {
                await expect(core.execute(sql, args)).rejects.toMatchObject({ sqlState: "57014" });
                expect(cancelled).toBe(true);
                reach.reach("cancelled loan");
              } else {
                const result = await core.execute(sql, args);
                if (result.kind !== "rows" || !("batches" in result))
                  throw new Error("missing group rows");
                expect(oracleRows(result)).toEqual((await pg.query(sql, args)).rows);
                reach.reach("PG exact");
                if (staging.partial) reach.reach("partial staged row");
              }
              expect(occupied).toBe(true);
              expect(requests.length).toBeGreaterThan(0);
              expect(requests.every((r) => r.bytes <= r.capacity)).toBe(true);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach(distinct ? "distinct parameters" : "repeated parameter");
              reach.reach(unicode ? "UTF8 widths" : "ASCII widths");
              reach.reach("pinned ordinary room");
              reach.reach("bounded sort loan");
            } finally {
              for (const m of mocks.reverse()) m.mockRestore();
              blocker.close();
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
            }
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [3, 800000, false, false, false],
            [4, 780000, true, false, false],
            [2, 800000, false, true, false],
            [3, 780000, true, true, false],
            [3, 800000, false, false, true],
            [4, 780000, true, true, true],
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
