/** PG float8pl/float4pl and float8_accum: signed zeros, ordered errors and special values. */
import { PGlite } from "@electric-sql/pglite";
import { beforeAll, afterAll, it, expect } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, WINDOW_FLOAT_REACH, WINDOW_FLOAT_DEMAND_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ArraySource, CollectSink, INT, DBL, batchesOf, type Row } from "./join-fixtures.js";
import type { Frame } from "./window-frames.js";
import { LimitOperator } from "./limit.js";
import { drive } from "./pipeline.js";
import { WindowOperator, windowMinimumBytes } from "./window.js";
import { AggregateOperator } from "./agg/aggregate.js";
import { OutputArena } from "./kernels/vectors.js";
import { UNCHARGED, col, numbers, valueAt, I32, F64 } from "./kernels/test-support.js";
import type { AggregateCall } from "../ir/ir.js";
import type { Operator } from "./pipeline.js";
import type { Batch } from "../vector/vector.js";
import type { PhysicalType } from "../types.js";
const demandReach = createReach("window float demand", WINDOW_FLOAT_DEMAND_REACH);
const reach = createReach("window float transitions", WINDOW_FLOAT_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
  demandReach.assertReached();
});
const bound = (kind: Frame["start"]["kind"], offset: number | null = null) => ({ kind, offset });
const frames: Array<[string, Frame]> = [
  [
    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
    {
      unit: "rows",
      start: bound("unbounded-preceding"),
      end: bound("unbounded-following"),
      exclude: "no-others",
    },
  ],
  [
    "ROWS UNBOUNDED PRECEDING",
    {
      unit: "rows",
      start: bound("unbounded-preceding"),
      end: bound("current-row"),
      exclude: "no-others",
    },
  ],
  [
    "ROWS 1 PRECEDING",
    { unit: "rows", start: bound("preceding", 1), end: bound("current-row"), exclude: "no-others" },
  ],
  [
    "ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING",
    { unit: "rows", start: bound("current-row"), end: bound("following", 1), exclude: "no-others" },
  ],
  [
    "ROWS CURRENT ROW",
    { unit: "rows", start: bound("current-row"), end: bound("current-row"), exclude: "no-others" },
  ],
  [
    "ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING",
    {
      unit: "rows",
      start: bound("preceding", 2),
      end: bound("preceding", 1),
      exclude: "no-others",
    },
  ],
  [
    "ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW",
    {
      unit: "rows",
      start: bound("unbounded-preceding"),
      end: bound("current-row"),
      exclude: "current-row",
    },
  ],
  [
    "ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW",
    {
      unit: "rows",
      start: bound("preceding", 2),
      end: bound("following", 1),
      exclude: "current-row",
    },
  ],
];
// Exercise the general default-RANGE route as well as explicit ROWS frames.
for (const [sql, frame] of frames.slice(0, 4))
  frames.push([sql.replace("ROWS", "RANGE"), { ...frame, unit: "range" }]);
function frameAt(index: number): [string, Frame] {
  const frame = frames[index];
  if (frame === undefined) throw new RangeError("Generated window frame is missing");
  return frame;
}
const scalar = (v: unknown): string => {
  if (v === null) return "NULL";
  if (Object.is(v, -0)) return "-0";
  if (typeof v === "number" || typeof v === "bigint" || typeof v === "string") return String(v);
  throw Error("scalar");
};
const errorCode = (error: unknown): unknown => {
  if (typeof error !== "object" || error === null) throw error;
  if ("sqlState" in error) return error.sqlState;
  if ("code" in error) return error.code;
  throw new Error("Expected a SQLSTATE-bearing error", { cause: error });
};
async function native(
  values: ReadonlyArray<number | null>,
  type: PhysicalType,
  fn: string,
  frame: Frame,
  streamRows: number,
  demand?: { limit: number; offset: number },
) {
  const types = [INT, type],
    governor = new Governor(64 << 20),
    spill = new MemorySpillService(),
    grant = await governor.admit({
      label: "float windows",
      minimumBytes: windowMinimumBytes(types),
      spillable: true,
    }),
    sink = new CollectSink(),
    arena = new OutputArena(UNCHARGED, 2048),
    limit =
      demand === undefined ? null : new LimitOperator(demand.limit, demand.offset, arena, sink),
    context = { grant, spill, queryId: 1, stopped: () => limit?.done === true },
    actor = new WindowOperator(
      {
        types,
        partitionBy: [],
        orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
        streamRows,
        functions: [{ fn, arg: 1, frame, type: fn === "sum" ? type : DBL }],
      },
      context,
      limit ?? sink,
    );
  try {
    const batches: Batch[] = batchesOf(
      [INT, DBL],
      values.map((x, id): Row => [id, x]),
      { batchRows: 3, skip: 3 },
    ).map((batch) => {
      if (type.kind !== "float32") return batch;
      return {
        ...batch,
        columns: batch.columns.map((vector, c) => {
          if (c !== 1) return vector;
          if (vector.form !== "flat" || vector.data.layout !== "f64") throw Error("float fixture");
          return {
            ...vector,
            type,
            data: { layout: "f32", values: Float32Array.from(vector.data.values) },
          };
        }),
      };
    });
    await drive(new ArraySource(batches), actor);
    await actor.closed;
    return { values: sink.rows.map((r) => scalar(r[2])) };
  } catch (error) {
    return { error: errorCode(error) };
  } finally {
    arena.close();
    actor.abort(Error("test complete"));
    await actor.closed;
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  }
}
async function expected(
  values: ReadonlyArray<number | null>,
  fn: string,
  frame: string,
  sqlType: string,
) {
  try {
    const data = values
      .map((v, id) => `(${String(id)},${v === null ? "NULL" : "'" + scalar(v) + "'::" + sqlType})`)
      .join(",");
    if (values.length === 0) return { values: [] };
    const result = await pg.query<{ v: number | null }>(
      `SELECT (${fn}(x) OVER(ORDER BY id ${frame}))::float8 v FROM(VALUES ${data}) t(id,x) ORDER BY id`,
    );
    return { values: result.rows.map((r) => scalar(r.v)) };
  } catch (error) {
    return { error: errorCode(error) };
  }
}
for (const seed of seedsFor("next-window-float-transitions", [0xb780, 0xb781]))
  for (const category of [
    "signed zero",
    "overflow",
    "special values",
    "float4",
    "page boundaries",
  ] as const)
    it(`${category} exact PG windows (${String(seed)})`, async () => {
      const random = mulberry32(seed);
      let cases: Array<Array<number | null>> = [];
      const type: PhysicalType = category === "float4" ? { kind: "float32" } : DBL,
        sqlType = category === "float4" ? "real" : "float8";
      if (category === "signed zero")
        cases = [[-0], [null, -0], [-0, -0], [-0, 0], [], [null, null, -0, null, -0]];
      if (category === "overflow")
        for (let position = 0; position < 5; position++)
          for (const sign of [1, -1]) {
            const prefix = Array.from({ length: position }, () => (random() < 0.4 ? null : 0));
            cases.push([...prefix, sign * Number.MAX_VALUE, sign * Number.MAX_VALUE, NaN]);
            cases.push([...prefix, sign * 1e200, -sign * 1e200, Infinity]);
          }
      if (category === "special values")
        cases = [
          [NaN, Number.MAX_VALUE, Number.MAX_VALUE],
          [Infinity, Number.MAX_VALUE, Number.MAX_VALUE],
          [-Infinity, Infinity, NaN],
          [null, -0, Infinity, -Infinity, NaN],
          [Number.MAX_VALUE, NaN, Number.MAX_VALUE],
          [Number.MAX_VALUE, Number.MAX_VALUE, Infinity],
        ];
      if (category === "float4")
        cases = [
          [-0],
          [16777216, 1, -16777216],
          [3.4028234663852886e38, 3.4028234663852886e38],
          [Infinity, -Infinity, NaN],
        ];
      if (category === "page boundaries") {
        const data: Array<number | null> = Array.from({ length: 1100 }, (_, i) =>
          i % 17 === 0 ? null : -0,
        );
        data[511] = Number.MAX_VALUE;
        data[512] = Number.MAX_VALUE;
        cases = [data];
      }
      for (const values of cases)
        for (const fn of ["sum", "avg"] as const)
          for (const [sql, frame] of frames) {
            reach.beginHistory();
            const want = await expected(values, fn, sql, sqlType);
            for (const streamRows of [100000, 1]) {
              const result = await native(values, type, fn, frame, streamRows);
              expect(
                result,
                JSON.stringify({
                  category,
                  fn,
                  sql,
                  streamRows,
                  values: values.length < 20 ? values : values.length,
                }),
              ).toEqual(want);
              reach.reach(streamRows === 1 ? "streamed" : "resident");
            }
            reach.reach(category);
            reach.reach(fn);
            if ("error" in want) reach.reach("22003");
            if (sql === "ROWS CURRENT ROW") reach.reach("unused prefix overflow");
            if (frame.exclude !== "no-others") reach.reach("exclusion fragments");
          }
    }, 60000);

for (const seed of seedsFor("next-window-float-group-check", [0xb782, 0xb783]))
  it(`GROUP BY float audit without changing aggregation (${String(seed)})`, async () => {
    for (const values of [
      [-0],
      [null, -0],
      [Number.MAX_VALUE, Number.MAX_VALUE],
      [1e200, -1e200],
      [NaN, Number.MAX_VALUE, Number.MAX_VALUE],
      [Infinity, -Infinity, NaN],
    ] as Array<Array<number | null>>)
      for (const fn of ["sum", "avg"] as const) {
        let want: unknown;
        const data = values
          .map((v, id) => `(${String(id)},${v === null ? "NULL" : "'" + scalar(v) + "'::float8"})`)
          .join(",");
        try {
          want = {
            values: (
              await pg.query<{ v: number | null }>(
                `SELECT ${fn}(x ORDER BY id) v FROM (VALUES ${data}) t(id,x)`,
              )
            ).rows.map((r) => scalar(r.v)),
          };
        } catch (error) {
          want = { error: errorCode(error) };
        }
        const arena = new OutputArena(UNCHARGED, 2048),
          output: string[] = [];
        const sink: Operator = {
          push(batch) {
            for (let row = 0; row < batch.length; row++)
              output.push(
                scalar(
                  valueAt(
                    batch.columns[1] ??
                      (() => {
                        throw Error("group output");
                      })(),
                    row,
                  ),
                ),
              );
            return "ready";
          },
          ready: () => Promise.resolve(),
          finish: () => undefined,
          abort: () => undefined,
        };
        const call: AggregateCall = {
          fn,
          args: [col(1, F64)],
          distinct: false,
          filter: null,
          orderBy: [],
          type: F64,
        };
        const actor = new AggregateOperator(
          {
            id: 1,
            op: "aggregate",
            input: { id: 0, op: "values", rows: [], output: [] },
            output: [],
            groupBy: [0],
            groupingSets: null,
            aggregates: [call],
          },
          { arena, input: [I32, F64], params: { slots: [] } },
          UNCHARGED,
          sink,
        );
        let actual: unknown;
        try {
          for (const value of values) {
            expect(
              actor.push({
                length: 1,
                selection: null,
                columns: [numbers(I32, [0]), numbers(F64, [value])],
              }),
            ).toBe("ready");
          }
          await actor.finish();
          actual = { values: output };
        } catch (error) {
          actor.abort(error);
          actual = { error: errorCode(error) };
        } finally {
          arena.close();
        }
        expect(actual).toEqual(want);
      }
  });

for (const seed of seedsFor("next-window-float-demand", [0xb790, 0xb791]))
  it(`respects generated frame demand, seed ${String(seed)}`, async () => {
    const random = mulberry32(seed);
    for (const fn of ["sum", "avg"])
      for (const streamRows of [1, 10000])
        for (const frameIndex of [0, 1, 2, 3, 6, 8, 9, 10, 11])
          for (const badAt of [1, 2, 4])
            for (const offset of [0, 1, 2])
              for (const limit of [1, 0, 2, 5]) {
                demandReach.beginHistory();
                demandReach.reach(streamRows === 1 ? "streamed" : "resident");
                demandReach.reach(fn === "sum" ? "sum" : "avg");
                demandReach.reach("LIMIT");
                if (offset > 0) demandReach.reach("OFFSET");
                if (badAt >= limit + offset) demandReach.reach("error after demand");
                else demandReach.reach("error within demand");
                demandReach.reach("NaN after overflow");
                const values = Array.from({ length: 6 }, (_, i) =>
                  i === badAt - 1 || i === badAt
                    ? 1e308
                    : i > badAt
                      ? NaN
                      : Math.floor(random() * 4),
                );
                const [sqlFrame, frame] = frameAt(frameIndex);
                let authority;
                try {
                  const data = values
                    .map((v, id) => `(${String(id)},'${scalar(v)}'::float8)`)
                    .join(",");
                  const result = await pg.query<{ v: number }>(
                    `SELECT ${fn}(x) OVER(ORDER BY id ${sqlFrame}) v FROM(VALUES ${data})t(id,x) LIMIT ${String(limit)} OFFSET ${String(offset)}`,
                  );
                  authority = { values: result.rows.map((r) => scalar(r.v)) };
                } catch (error) {
                  authority = { error: errorCode(error) };
                }
                expect(
                  await native(values, DBL, fn, frame, streamRows, { limit, offset }),
                  JSON.stringify({ fn, streamRows, frameIndex, badAt, offset, limit }),
                ).toEqual(authority);
              }
  }, 120000);

for (const seed of seedsFor("next-window-float-cursor-demand", [0xb794, 0xb795]))
  it(`closes a cursor before a future error (${String(seed)})`, async () => {
    const random = mulberry32(seed);
    for (const streamRows of [1, 10000])
      for (const badAt of [1, 2049, 3001]) {
        demandReach.beginHistory();
        demandReach.reach("cursor close");
        if (badAt > 2048) demandReach.reach("batch boundary");
        const fn = random() < 0.5 ? "sum" : "avg";
        const governor = new Governor(64 << 20),
          spill = new MemorySpillService();
        const grant = await governor.admit({
          label: "cursor",
          minimumBytes: windowMinimumBytes([INT, DBL]),
          spillable: true,
        });
        const sink = new CollectSink();
        let entered!: () => void, resume!: () => void;
        const first = new Promise<void>((resolve) => {
          entered = resolve;
        });
        const gate = new Promise<void>((resolve) => {
          resume = resolve;
        });
        const consumer: Operator = {
          push(batch) {
            sink.push(batch);
            entered();
            return "blocked";
          },
          ready: () => gate,
          finish: () => sink.finish(),
          abort: (error) => sink.abort(error),
        };
        const actor = new WindowOperator(
          {
            types: [INT, DBL],
            partitionBy: [],
            orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
            functions: [{ fn, arg: 1, type: DBL, frame: frameAt(1)[1] }],
            streamRows,
          },
          { grant, spill, queryId: 1 },
          consumer,
        );
        const values = Array.from({ length: badAt + 2 }, (_, i) =>
          i === badAt - 1 || i === badAt ? 1e308 : i > badAt ? NaN : 0,
        );
        const work = Promise.resolve().then(() =>
          drive(
            new ArraySource(
              batchesOf(
                [INT, DBL],
                values.map((x, id) => [id, x]),
              ),
            ),
            actor,
          ),
        );
        const settled = work.then(
          () => ({ done: true }),
          (error: unknown) => ({ error }),
        );
        try {
          expect(
            await Promise.race([
              first.then(() => "prefix"),
              settled.then(() => "failed before prefix"),
            ]),
          ).toBe("prefix");
          expect(sink.rows.length).toBeGreaterThan(0);
          expect(sink.rows.length).toBeLessThanOrEqual(badAt);
          // PG's cursor may likewise close after fetching this prefix, without evaluating more.
          await pg.exec("BEGIN");
          const data = values.map((x, id) => `(${String(id)},'${scalar(x)}'::float8)`).join(",");
          await pg.exec(
            `DECLARE c CURSOR FOR SELECT ${fn}(x) OVER(ORDER BY id ROWS UNBOUNDED PRECEDING) v FROM(VALUES ${data})t(id,x)`,
          );
          const expected = await pg.query<{ v: number }>(
            `FETCH ${String(sink.rows.length)} FROM c`,
          );
          expect(sink.rows.map((r) => scalar(r[2]))).toEqual(expected.rows.map((r) => scalar(r.v)));
          await pg.exec("CLOSE c; COMMIT");
          const reason = Error("cursor closed");
          actor.abort(reason);
          resume();
          expect(await settled).toEqual({ error: reason });
        } finally {
          resume();
          actor.abort(Error("test complete"));
          await settled;
          await actor.closed;
          await pg.exec("ROLLBACK");
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
        }
      }
  }, 120000);
