/** Row prefixes survive an expression error identically under pinned pressure and ordinary credit. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { QueryRuntime } from "../exec/run.js";
import type { Batch } from "../vector/vector.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- applied to the original receiver
const reserve = QueryRuntime.prototype.reserve;
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("executor-expression-errors", [0xb7b9, 0xb7ba]))(
  "keeps error and delivered prefix independent of pressure seed%i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 5 }),
          fc.boolean(),
          fc.boolean(),
          async (n, competing, pressure) => {
            const key = "CASE WHEN i=1 THEN $1::text ELSE 'k' END";
            const sql = competing
              ? `SELECT 10/(2-i) x, 2147483647+i y, ${key} k FROM generate_series(1,${String(n)})g(i)`
              : `SELECT 10/(${String(n)}-i) x, ${key} k FROM generate_series(1,${String(n)})g(i)`;
            const expected: Array<Record<string, unknown>> = [];
            let pgError: string | undefined;
            await pg.exec(
              `BEGIN; DECLARE c CURSOR FOR ${sql.replace("$1::text", "repeat('x',800001)")}`,
            );
            try {
              for (;;) {
                const rows = (await pg.query<Record<string, unknown>>("FETCH 1 FROM c")).rows;
                if (!rows.length) break;
                expected.push(...rows);
              }
            } catch (error) {
              pgError = (error as { code: string }).code;
            } finally {
              await pg.exec("ROLLBACK");
            }
            const governor = new Governor(128 << 20);
            const competitor = await governor.admit({
              label: "pinned",
              minimumBytes: 0,
              spillable: true,
            });
            let pinned = false;
            const prepare = core.executor.prepare.bind(core.executor);
            const mockReserve = vi
              .spyOn(QueryRuntime.prototype, "reserve")
              .mockImplementation(async function (this: QueryRuntime, ...args) {
                await reserve.apply(this, args);
                if (this.request.governor === governor && pressure && !pinned) {
                  expect(
                    competitor.allocate(
                      governor.capBytes - governor.reserveBytes - governor.usedBytes,
                    ),
                  ).toBe(true);
                  pinned = true;
                }
              });
            const mockPrepare = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
              const prepared = prepare(plan);
              return {
                ...prepared,
                execute: (request) =>
                  prepared.execute({ ...request, governor, signal: AbortSignal.timeout(30000) }),
              };
            });
            const prepared = core.prepare(sql);
            const run = core.start(prepared, ["x".repeat(800001)]);
            const batches: Batch[] = [];
            let actualError: string | undefined;
            try {
              for (;;) {
                const frame = await run.execution.results.next();
                if (!frame) break;
                try {
                  const payload = structuredClone(frame.payload);
                  batches.push({
                    length: payload.rowCount,
                    selection: null,
                    columns: payload.columns.map((vector, c) => {
                      if (vector.form !== "flat") throw new Error("Expected copied flat result");
                      const type = prepared.columns[c]?.type;
                      if (!type) throw new Error("Missing result type");
                      return { ...vector, type };
                    }),
                  });
                } finally {
                  frame.release();
                }
              }
              await run.execution.executed;
              await run.execution.delivered;
            } catch (error) {
              actualError = (error as { sqlState: string }).sqlState;
            } finally {
              run.execution.cancel();
              await Promise.allSettled([run.execution.executed, run.execution.delivered]);
              run.release();
              mockPrepare.mockRestore();
              mockReserve.mockRestore();
              competitor.close();
            }
            expect(actualError).toBe(pgError);
            expect(oracleRows({ columns: prepared.columns, batches })).toEqual(expected);
            expect(pinned).toBe(pressure);
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 8,
          endOnFailure: true,
          examples: [
            [2, true, false],
            [2, true, true],
            [3, false, false],
            [3, false, true],
          ],
        },
      );
    } finally {
      await core.close();
      await pg.close();
    }
  },
  60000,
);
