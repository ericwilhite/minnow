/** PG nextval proves the number and order of demanded volatile default evaluations. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, WINDOW_VOLATILE_DEFAULT_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ArraySource, CollectSink, INT, batchesOf } from "./join-fixtures.js";
import { WindowOperator, windowMinimumBytes } from "./window.js";
import { LimitOperator } from "./limit.js";
import { drive, yieldTask, type Operator } from "./pipeline.js";
import { Charge } from "./spill-pool.js";
import { OutputArena } from "./kernels/vectors.js";
for (const seed of seedsFor("next-window-volatile-default", [0xb7a6, 0xb7a7]))
  it(`matches volatile PG defaults and counts (${String(seed)})`, async () => {
    const pg = new PGlite(),
      random = mulberry32(seed);
    const reach = createReach(`volatile default ${String(seed)}`, WINDOW_VOLATILE_DEFAULT_REACH);
    try {
      await pg.waitReady;
      await pg.exec("CREATE SEQUENCE shift_calls");
      for (const n of [7, 24])
        for (const fn of ["lag", "lead"])
          for (const amount of [-2, 1, 999])
            for (const streamRows of [1, 10000])
              for (const limit of [0, 1, 3, null]) {
                reach.beginHistory();
                reach.reach(fn === "lag" ? "lag" : "lead");
                reach.reach(streamRows === 1 ? "streamed" : "resident");
                const offset = random() < 0.5 ? 0 : 1;
                if (limit === 0) reach.reach("zero demand");
                if (offset) reach.reach("OFFSET");
                const rows = Array.from({ length: n }, (_, id): [number, number | null] => [
                  id,
                  id % 4 === 0 ? null : id + 10,
                ]);
                await pg.exec("ALTER SEQUENCE shift_calls RESTART WITH 1");
                const data = rows
                  .map((r) => `(${String(r[0])},${r[1] === null ? "NULL" : String(r[1])})`)
                  .join(",");
                const sql = `SELECT ${fn}(x,${String(amount)},nextval('shift_calls')::int) OVER(ORDER BY id) v FROM(VALUES ${data})t(id,x) ${limit === null ? "" : `LIMIT ${String(limit)}`} OFFSET ${String(offset)}`;
                const expected = (await pg.query<{ v: number | null }>(sql)).rows.map((r) => r.v);
                const counter = (
                  await pg.query<{ last_value: number; is_called: boolean }>(
                    "SELECT last_value,is_called FROM shift_calls",
                  )
                ).rows[0];
                if (counter === undefined) throw Error("missing PG counter");
                const expectedCalls = counter.is_called ? counter.last_value : 0;
                if (expectedCalls === 0) reach.reach("no default evaluation");
                else reach.reach("default evaluation");
                const types = [INT, INT],
                  gov = new Governor(64 << 20),
                  spill = new MemorySpillService();
                const grant = await gov.admit({
                  label: "volatile shift",
                  minimumBytes: windowMinimumBytes(types) + 65536,
                  spillable: true,
                });
                const compiler = await gov.admit({
                  label: "volatile default",
                  minimumBytes: 65536,
                  spillable: false,
                });
                const arena = new OutputArena(new Charge(compiler), 2048),
                  out = arena.flat(INT).vector;
                if (out.data.layout !== "i32") throw Error("int default");
                const values = out.data.values;
                const sink = new CollectSink();
                let borrowed: Parameters<Operator["push"]>[0] | null = null;
                const delayed: Operator = {
                  push(batch) {
                    borrowed = batch;
                    return "blocked";
                  },
                  async ready() {
                    await yieldTask();
                    if (borrowed === null) throw Error("missing borrowed rows");
                    sink.push(borrowed);
                    borrowed = null;
                  },
                  finish: () => sink.finish(),
                  abort: (r) => {
                    borrowed = null;
                    sink.abort(r);
                  },
                };
                const take = new LimitOperator(
                  limit ?? Number.MAX_SAFE_INTEGER,
                  offset,
                  arena,
                  delayed,
                );
                let calls = 0;
                const actor = new WindowOperator(
                  {
                    types,
                    partitionBy: [],
                    orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
                    streamRows,
                    functions: [
                      {
                        fn,
                        arg: 1,
                        amount,
                        type: INT,
                        frame: null,
                        defaultValue(batch) {
                          out.length = batch.length;
                          const count = batch.selection?.count ?? batch.length;
                          for (let i = 0; i < count; i++)
                            values[batch.selection?.rows[i] ?? i] = ++calls;
                          return out;
                        },
                      },
                    ],
                  },
                  { grant, spill, queryId: 1, stopped: () => take.done },
                  take,
                );
                try {
                  await drive(
                    new ArraySource(batchesOf(types, rows, { batchRows: 3, skip: 2 })),
                    actor,
                  );
                  expect(
                    sink.rows.map((r) => r[2]),
                    sql,
                  ).toEqual(expected);
                  expect(calls, sql).toBe(expectedCalls);
                } finally {
                  actor.abort(Error("test complete"));
                  await actor.closed;
                  arena.close();
                  compiler.close();
                  expect(gov.usedBytes).toBe(0);
                  expect(spill.bytesInUse).toBe(0);
                }
              }
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
