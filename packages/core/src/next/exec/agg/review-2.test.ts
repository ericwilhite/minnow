/**
 * Review B4-p2b-2: each finding's probe as a committed regression test. Law: PostgreSQL via
 * PGlite for NUMERIC values and limits; memory/governor.ts "Accounting unit" (`bigintBytes` per
 * retained BigInt) for charges; exec/pipeline.ts and R23 for deadlines and cancellation.
 */
import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "vitest";
import type { AggregateCall } from "../../ir/ir.js";
import { bigintBytes } from "../../memory/governor.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { Batch } from "../../vector/vector.js";
import { NumericOutOfRangeError } from "../errors.js";
import {
  I32,
  I64,
  UNCHARGED,
  col,
  decimal,
  decimals,
  numbers,
  valueAt,
} from "../kernels/test-support.js";
import { IDENTITY, OutputArena } from "../kernels/vectors.js";
import type { Operator, PushResult } from "../pipeline.js";
import { AggregateOperator, type AggregateOptions } from "./aggregate.js";
import { accumulatorFor } from "./states.js";
import { GroupValues } from "./values.js";

const D = decimal(null, null);

function counting(): ByteCharge & { held: number } {
  const charge = {
    held: 0,
    allocate(bytes: number) {
      charge.held += bytes;
      return true;
    },
    free(bytes: number) {
      charge.held -= bytes;
    },
  };
  return charge;
}

const sinkOf = (push: (batch: Batch) => PushResult): Operator => ({
  push,
  ready: () => Promise.resolve(),
  finish: () => undefined,
  abort: () => undefined,
});

function avgOperator(
  charge: ByteCharge,
  sink: Operator,
  options?: AggregateOptions,
): AggregateOperator {
  const avg: AggregateCall = {
    fn: "avg",
    type: D,
    args: [col(1, D)],
    distinct: false,
    filter: null,
    orderBy: [],
  };
  return new AggregateOperator(
    {
      id: 1,
      op: "aggregate",
      output: [],
      input: { id: 0, op: "values", output: [], rows: [] },
      groupBy: [0],
      groupingSets: null,
      aggregates: [avg],
    },
    { arena: new OutputArena(UNCHARGED, 2048), input: [I32, D], params: { slots: [] } },
    charge,
    sink,
    options,
  );
}

/** 2,048 groups, each averaging `x`, in 16 input batches of 128 rows. */
function feed(operator: AggregateOperator, x: bigint, groups = 2048): void {
  for (let from = 0; from < groups; from += 128)
    operator.push({
      length: 128,
      selection: null,
      columns: [
        numbers(
          I32,
          Array.from({ length: 128 }, (_, i) => from + i),
        ),
        decimals(
          D,
          Array.from({ length: 128 }, () => [x, 0] as const),
          "wide",
          0,
        ),
      ],
    });
}

describe("review B4-p2b-2", () => {
  it("1. a huge SUM's state is charged by bigintBytes, grows with it, and returns at close", () => {
    const charge = counting();
    const sum = accumulatorFor("sum", D, D, charge);
    sum.grow(1);
    const x = 10n ** 9999n;
    sum.update(decimals(D, [[x, 0]], "wide", 0), new Int32Array(1), IDENTITY, 1, true);
    expect(charge.held).toBeGreaterThanOrEqual(bigintBytes(x));
    const before = charge.held;
    // Adding more makes the offset larger: the size change is charged.
    const y = 10n ** 20000n;
    sum.update(decimals(D, [[y, 0]], "wide", 0), new Int32Array(1), IDENTITY, 1, true);
    expect(charge.held - before).toBeGreaterThanOrEqual(bigintBytes(x + y) - bigintBytes(x));
    sum.close();
    expect(charge.held).toBe(0);
  });

  it("1. a MIN/MAX replacement by a larger (or smaller) coefficient charges the size change", () => {
    const charge = counting();
    const values = new GroupValues(D, charge);
    values.append(decimals(D, [[1n << 129n, 0]], "wide", 0), 0);
    const before = charge.held;
    const x = 10n ** 9999n;
    values.set(0, decimals(D, [[x, 0]], "wide", 0), 0);
    expect(charge.held - before).toBeGreaterThanOrEqual(bigintBytes(x) - bigintBytes(1n << 129n));
    values.set(0, decimals(D, [[5n, 0]], "wide", 0), 0);
    expect(charge.held).toBeLessThan(before);
    expect(valueAt(values.emit(0, 1), 0)).toEqual([5n, 0]);
    values.close();
    expect(charge.held).toBe(0);
  });

  it("2. AVG over huge coefficients emits byte-bounded batches (real BigInt sizes)", async () => {
    const charge = counting();
    let oversized = 0;
    let rows = 0;
    const op = avgOperator(
      charge,
      sinkOf((batch) => {
        const v = batch.columns[1];
        const d = v?.form === "flat" ? v.data : null;
        if (d?.layout === "decimal" && d.coefficients.repr === "wide") {
          const bytes = d.coefficients.values.reduce((s, c) => s + bigintBytes(c), 0);
          if (batch.length > 1 && bytes > 1 << 20) oversized += 1;
        }
        rows += batch.length;
        return "ready";
      }),
      // A frozen clock: only the byte bound may cut batches here (not the deadline).
      { now: () => 0 },
    );
    feed(op, 10n ** 9999n);
    await op.finish();
    expect(rows).toBe(2048);
    expect(oversized).toBe(0);
    expect(charge.held).toBe(0);
  }, 120_000);

  it("3. AVG's per-group divisions stop at the deadline mid-batch and resume after a task", async () => {
    let tick = 0;
    const lengths: number[] = [];
    let timerRan = false;
    let timerBeforeLast = false;
    const op = avgOperator(
      UNCHARGED,
      sinkOf((batch) => {
        lengths.push(batch.length);
        if (timerRan) timerBeforeLast = true;
        return "ready";
      }),
      // 1 ms per clock reading, 4 ms slices: a few huge divisions per slice.
      { now: () => tick++, yieldIntervalMs: 4 },
    );
    feed(op, 10n ** 2000n, 256);
    setImmediate(() => {
      timerRan = true;
    });
    const done = op.finish();
    expect(done).toBeInstanceOf(Promise);
    await done;
    expect(lengths.reduce((a, b) => a + b, 0)).toBe(256);
    // Batches far smaller than the byte target: the deadline, not the size, cut them.
    expect(Math.max(...lengths)).toBeLessThan(256);
    expect(timerBeforeLast).toBe(true);
  }, 120_000);

  it("3. finish after abort rejects with the abort's reason (R23)", async () => {
    const reason = new Error("cancel review");
    const op = new AggregateOperator(
      {
        id: 1,
        op: "aggregate",
        output: [],
        input: { id: 0, op: "values", output: [], rows: [] },
        groupBy: [],
        groupingSets: null,
        aggregates: [],
      },
      { arena: new OutputArena(UNCHARGED, 2048), input: [], params: { slots: [] } },
      UNCHARGED,
      sinkOf(() => "ready"),
    );
    op.abort(reason);
    await expect(Promise.resolve().then(() => op.finish())).rejects.toBe(reason);
  });

  it("4. a selection-buffer failure after a DISTINCT set unwinds every charge", () => {
    const charge = counting();
    const arena = new OutputArena(UNCHARGED, 2048);
    const Real = globalThis.Uint16Array;
    const error = new RangeError("injected selection allocation failure");
    globalThis.Uint16Array = new Proxy(Real, {
      construct(target, args: [number]) {
        if (args[0] === 2048) throw error;
        return Reflect.construct(target, args) as object;
      },
    });
    try {
      expect(
        () =>
          new AggregateOperator(
            {
              id: 1,
              op: "aggregate",
              output: [],
              input: { id: 0, op: "values", output: [], rows: [] },
              groupBy: [],
              groupingSets: null,
              aggregates: [
                {
                  fn: "count",
                  type: I64,
                  args: [col(0, I32)],
                  distinct: true,
                  filter: null,
                  orderBy: [],
                },
              ],
            },
            { arena, input: [I32], params: { slots: [] } },
            charge,
            sinkOf(() => "ready"),
          ),
      ).toThrow(error);
    } finally {
      globalThis.Uint16Array = Real;
      arena.close();
    }
    expect(charge.held).toBe(0);
  });

  it("5. NUMERIC results past PostgreSQL's limits are 22003 at emission, as PostgreSQL", async () => {
    const pg = new PGlite();
    try {
      const x = 10n ** 131072n - 1n;
      const text = x.toString();
      // SUM(x, x) and AVG(x, x) overflow; SUM(x, x, -x) comes back into range and is fine.
      for (const fn of ["sum", "avg"]) {
        await expect(
          pg.query(`select ${fn}(x) from (values ('${text}'::numeric), ('${text}'::numeric)) t(x)`),
        ).rejects.toMatchObject({ code: "22003" });
        const a = accumulatorFor(fn, D, D, UNCHARGED);
        a.grow(1);
        a.update(
          decimals(
            D,
            [
              [x, 0],
              [x, 0],
            ],
            "wide",
            0,
          ),
          new Int32Array(2),
          IDENTITY,
          2,
          true,
        );
        expect(() => a.emit(0, 1)).toThrow(NumericOutOfRangeError);
        a.close();
      }
      const back = await pg.query<{ n: number }>(
        `select length(sum(x)::text) n from (values ('${text}'::numeric), ('${text}'::numeric), ('-${text}'::numeric)) t(x)`,
      );
      expect(back.rows[0]?.n).toBe(131_072);
      const sum = accumulatorFor("sum", D, D, UNCHARGED);
      sum.grow(1);
      sum.update(
        decimals(
          D,
          [
            [x, 0],
            [x, 0],
            [-x, 0],
          ],
          "wide",
          0,
        ),
        new Int32Array(3),
        IDENTITY,
        3,
        true,
      );
      expect(valueAt(sum.emit(0, 1), 0)).toEqual([x, 0]);
      sum.close();
    } finally {
      await pg.close();
    }
  }, 180_000);
});
