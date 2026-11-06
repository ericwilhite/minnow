/**
 * Review B4-p2b-3: the probes as committed regression tests. Law: memory/governor.ts "Accounting
 * unit" (`bigintBytes` per BigInt held in operator state, charged before it is kept); an
 * operator's close and abort release its state (exec/pipeline.ts); R23 for yields.
 */
import { describe, expect, it } from "vitest";
import type { AggregateCall } from "../../ir/ir.js";
import { bigintBytes } from "../../memory/governor.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { Batch } from "../../vector/vector.js";
import { I32, UNCHARGED, col, decimal, decimals, numbers } from "../kernels/test-support.js";
import { IDENTITY, OutputArena } from "../kernels/vectors.js";
import type { Operator, PushResult } from "../pipeline.js";
import { AggregateOperator, type AggregateOptions } from "./aggregate.js";
import { accumulatorFor } from "./states.js";

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

/** Captures the arrays a callback pushes BigInts into (the AVG cache), restoring `push` after. */
function capturingBigIntArrays<T>(run: () => T): { result: T; arrays: bigint[][] } {
  const arrays: bigint[][] = [];
  const push = Array.prototype.push;
  Array.prototype.push = function (this: unknown[], ...items: unknown[]) {
    if (typeof items[0] === "bigint" && !arrays.includes(this as bigint[]))
      arrays.push(this as bigint[]);
    return push.apply(this, items);
  };
  try {
    return { result: run(), arrays };
  } finally {
    Array.prototype.push = push;
  }
}

describe("review B4-p2b-3", () => {
  it("prepared AVG results are charged by bigintBytes before they are kept, and cleared at close", () => {
    const charge = counting();
    const avg = accumulatorFor("avg", D, D, charge);
    avg.grow(128);
    const x = 10n ** 9999n;
    avg.update(
      decimals(
        D,
        Array.from({ length: 128 }, () => [x, 0] as const),
        "wide",
        0,
      ),
      Int32Array.from({ length: 128 }, (_, i) => i),
      IDENTITY,
      128,
      false,
    );
    const before = charge.held;
    const { result: prepared, arrays } = capturingBigIntArrays(() =>
      avg.prepare?.(0, 128, () => false),
    );
    expect(prepared).toBe(128);
    const cache = arrays[0] ?? [];
    const bytes = cache.reduce((n, c) => n + bigintBytes(c), 0);
    expect(charge.held - before).toBeGreaterThanOrEqual(bytes);
    avg.close();
    expect(cache.length).toBe(0);
    expect(charge.held).toBe(0);
  });

  it("an abort while yielding leaves no prepared result and no charge", async () => {
    const charge = counting();
    let tick = 0;
    const reason = new Error("abort cached results");
    const op = avgOperator(
      charge,
      sinkOf(() => {
        tick = 100; // the slice ends after every batch
        return "ready";
      }),
      { now: () => tick, yieldIntervalMs: 4, targetBytes: 4096 },
    );
    feed(op, 10n ** 2000n, 128);
    const { result: done, arrays } = capturingBigIntArrays(() => op.finish());
    // Yielding now: with one AVG column, whatever was prepared went out with its batch. (With
    // several, the others may keep up to a batch's worth, charged, until the next batch.)
    expect(done).toBeInstanceOf(Promise);
    for (const cache of arrays) expect(cache.length).toBe(0);
    op.abort(reason);
    await expect(done).rejects.toBe(reason);
    for (const cache of arrays) expect(cache.length).toBe(0);
    expect(charge.held).toBe(0);
  });

  it("prepares only what a byte-trimmed or grant-refused batch holds", async () => {
    let refuseAbove = Infinity;
    const charge = {
      held: 0,
      allocate(bytes: number) {
        if (bytes > refuseAbove) return false;
        charge.held += bytes;
        return true;
      },
      free(bytes: number) {
        charge.held -= bytes;
      },
    };
    let rows = 0;
    let peak = 0;
    const op = avgOperator(
      charge,
      sinkOf((batch) => {
        rows += batch.length;
        peak = Math.max(peak, charge.held);
        return "ready";
      }),
      { now: () => 0, targetBytes: 16 * 1024 },
    );
    feed(op, 10n ** 2000n, 512);
    const state = charge.held;
    refuseAbove = 8 * 1024; // smaller still than the byte target: the grant halves batches
    await op.finish();
    expect(rows).toBe(512);
    // Output batches and their prepared results stay within a few batches' bytes of the state.
    expect(peak - state).toBeLessThan(64 * 1024);
    expect(charge.held).toBe(0);
  });
});
