/** leadlag_common reads a default only at an out-of-range output row. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, WINDOW_SHIFT_DELIVERY_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ArraySource, CollectSink, INT, batchesOf } from "./join-fixtures.js";
import { WindowOperator, windowMinimumBytes } from "./window.js";
import { LimitOperator } from "./limit.js";
import { drive, yieldTask, type Operator } from "./pipeline.js";
import { Charge } from "./spill-pool.js";
import { compileExpr } from "./kernels/compile.js";
import { OutputArena } from "./kernels/vectors.js";
import { call, col, lit, numbers } from "./kernels/test-support.js";

const sqlState = (error: unknown): unknown => {
  if (typeof error === "object" && error !== null) {
    if ("sqlState" in error) return error.sqlState;
    if ("code" in error) return error.code;
  }
  throw error;
};
for (const seed of seedsFor("next-window-shift-delivery", [0xb7a2, 0xb7a3]))
  it(`delivers generated lazy defaults through both routes (${String(seed)})`, async () => {
    const pg = new PGlite();
    const random = mulberry32(seed);
    const reach = createReach(`shift delivery ${String(seed)}`, WINDOW_SHIFT_DELIVERY_REACH);
    try {
      await pg.waitReady;
      for (const n of [24, 2051])
        for (const fn of ["lag", "lead"])
          for (const streamRows of [1, 10000])
            for (const amount of [-2, 1, 999])
              for (const limit of [1, null]) {
                reach.beginHistory();
                reach.reach(fn === "lag" ? "lag" : "lead");
                reach.reach(streamRows === 1 ? "streamed" : "resident");
                reach.reach("NULL values");
                if (n > 2048) reach.reach("page boundary");
                if (limit === 1) reach.reach("default after demand");
                const rows = Array.from({ length: n }, (_, id): [number, number | null, number] => [
                  id,
                  id % 4 === 0 ? null : id - 7,
                  id - 1,
                ]);
                const sorted = [...rows];
                for (let i = rows.length - 1; i > 0; i--) {
                  const j = Math.floor(random() * (i + 1));
                  const a = rows[i],
                    b = rows[j];
                  if (a === undefined || b === undefined)
                    throw new RangeError("Invalid shuffled window row");
                  rows[i] = b;
                  rows[j] = a;
                }
                const data = sorted
                  .map((row) => `(${row.map((v) => (v === null ? "NULL" : String(v))).join(",")})`)
                  .join(",");
                let expected;
                try {
                  expected = {
                    values: (
                      await pg.query<{ v: number | null }>(
                        `SELECT ${fn}(x,${String(amount)},100/d) OVER(ORDER BY id) v FROM(VALUES ${data})t(id,x,d) ${limit === null ? "" : `LIMIT ${String(limit)}`}`,
                      )
                    ).rows.map((row) => row.v),
                  };
                } catch (error) {
                  expected = { error: sqlState(error) };
                }
                const types = [INT, INT, INT],
                  governor = new Governor(64 << 20),
                  spill = new MemorySpillService();
                const grant = await governor.admit({
                  label: "lazy shift",
                  minimumBytes: windowMinimumBytes(types) + 65536,
                  spillable: true,
                });
                const compilerGrant = await governor.admit({
                  label: "default compiler",
                  minimumBytes: 65536,
                  spillable: false,
                });
                const arena = new OutputArena(new Charge(compilerGrant), 2048);
                const expression = compileExpr(
                  call("div", INT, lit(numbers(INT, [100])), col(2, INT)),
                  { arena, input: types, params: { slots: [] } },
                );
                const sink = new CollectSink();
                let borrowed: Parameters<Operator["push"]>[0] | null = null;
                const consumer: Operator = {
                  push(batch) {
                    if (borrowed !== null) throw Error("push while borrowed");
                    borrowed = batch;
                    return "blocked";
                  },
                  async ready() {
                    await yieldTask();
                    if (borrowed === null) throw Error("missing borrowed batch");
                    sink.push(borrowed);
                    borrowed = null;
                  },
                  finish: () => sink.finish(),
                  abort: (reason) => {
                    borrowed = null;
                    sink.abort(reason);
                  },
                };
                const take = limit === null ? null : new LimitOperator(limit, 0, arena, consumer);
                const actor = new WindowOperator(
                  {
                    types,
                    partitionBy: [],
                    orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
                    sortPath: random() < 0.5 ? "sort" : "grouped",
                    streamRows,
                    functions: [
                      {
                        fn,
                        arg: 1,
                        amount,
                        type: INT,
                        frame: null,
                        defaultValue: (batch) => expression.evaluate(batch, batch.selection),
                      },
                    ],
                  },
                  { grant, spill, queryId: 1, stopped: () => take?.done === true },
                  take ?? consumer,
                );
                let actual;
                try {
                  await drive(
                    new ArraySource(batchesOf(types, rows, { batchRows: 137, skip: 3 })),
                    actor,
                  );
                  actual = { values: sink.rows.map((row) => row[3]) };
                } catch (error) {
                  actual = { error: sqlState(error) };
                } finally {
                  actor.abort(Error("test complete"));
                  await actor.closed;
                  arena.close();
                  compilerGrant.close();
                  expect(governor.usedBytes).toBe(0);
                  expect(spill.bytesInUse).toBe(0);
                }
                if (streamRows === 1) expect(actor.profile.streamedPartitions).toBeGreaterThan(0);
                expect(actual, JSON.stringify({ n, fn, streamRows, amount, limit })).toEqual(
                  expected,
                );
              }
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
