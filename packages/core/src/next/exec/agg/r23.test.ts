/**
 * Ruling R23 for the aggregate operator: output in bounded slices with task yields (a zero-delay
 * timer scheduled before `finish` runs before it completes), the abort signal checked at every
 * slice and after backpressure (an aborted signal rejects with its reason), and no shared
 * scratch across suspensions (two interleaved instances on one worker give each one's answer).
 */
import { describe, expect, it } from "vitest";
import type { AggregateCall } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import type { Batch } from "../../vector/vector.js";
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
import { OutputArena } from "../kernels/vectors.js";
import type { Operator, PushResult } from "../pipeline.js";
import { AggregateOperator, type AggregateOptions } from "./aggregate.js";

const D = decimal(null, null);

const call = (
  fn: AggregateCall["fn"],
  type: PhysicalType,
  input: PhysicalType | null,
  distinct = false,
): AggregateCall => ({
  fn,
  args: input === null ? [] : [col(1, input)],
  distinct,
  filter: null,
  orderBy: [],
  type,
});

function operator(
  groupBy: number[],
  aggregates: AggregateCall[],
  sink: Operator,
  options: AggregateOptions,
): AggregateOperator {
  return new AggregateOperator(
    {
      id: 1,
      op: "aggregate",
      output: [],
      input: { id: 0, op: "values", output: [], rows: [] },
      groupBy,
      groupingSets: null,
      aggregates,
    },
    { arena: new OutputArena(UNCHARGED, 2048), input: [I32, D], params: { slots: [] } },
    UNCHARGED,
    sink,
    options,
  );
}

/** A sink collecting rows as JS values; `blocked` returns "blocked" for its nth push. */
function collector(blocked = -1, ready: () => Promise<void> = () => Promise.resolve()) {
  const rows: unknown[][] = [];
  let pushes = 0;
  const sink: Operator = {
    push(batch: Batch): PushResult {
      for (let r = 0; r < batch.length; r++) rows.push(batch.columns.map((c) => valueAt(c, r)));
      pushes += 1;
      return pushes === blocked ? "blocked" : "ready";
    },
    ready,
    finish: () => undefined,
    abort: () => undefined,
  };
  return { sink, rows, pushes: () => pushes };
}

function batch(from: number, count: number, groups: number, value: (row: number) => bigint): Batch {
  return {
    length: count,
    selection: null,
    columns: [
      numbers(
        I32,
        Array.from({ length: count }, (_, i) => (from + i) % groups),
      ),
      decimals(
        D,
        Array.from({ length: count }, (_, i) => [value(from + i), 0] as const),
        "int128",
      ),
    ],
  };
}

const AGGREGATES = [call("count_star", I64, null), call("avg", D, D), call("count", I64, D, true)];

describe("R23: the aggregate operator's finish", () => {
  it("lets a zero-delay timer run before a finish longer than one slice completes", async () => {
    let tick = 0;
    const { sink, rows } = collector();
    // A clock advancing 1 ms per reading: the 4 ms slices end every few batches.
    const op = operator([0], AGGREGATES, sink, { now: () => tick++ });
    for (let from = 0; from < 50_000; from += 2048)
      op.push(batch(from, Math.min(2048, 50_000 - from), 20_000, (r) => BigInt(r) * 10n ** 30n));
    let timer = false;
    setTimeout(() => {
      timer = true;
    }, 0);
    let timerBeforeEnd = false;
    const done = op.finish();
    expect(done).toBeInstanceOf(Promise);
    await Promise.resolve(done).then(() => {
      timerBeforeEnd = timer;
    });
    expect(timerBeforeEnd).toBe(true);
    expect(rows.length).toBe(20_000);
  });

  it("rejects with the signal's reason when aborted before finish", async () => {
    const controller = new AbortController();
    const reason = new Error("cancelled before finish");
    const { sink, pushes } = collector();
    const op = operator([0], AGGREGATES, sink, { signal: controller.signal });
    op.push(batch(0, 100, 10, (r) => BigInt(r)));
    controller.abort(reason);
    await expect(Promise.resolve().then(() => op.finish())).rejects.toBe(reason);
    expect(pushes()).toBe(0);
  });

  it("checks the signal again after downstream backpressure", async () => {
    const controller = new AbortController();
    const reason = new Error("cancelled while blocked");
    let release = (): void => undefined;
    const { sink, pushes } = collector(
      1,
      () => new Promise<void>((resolve) => (release = resolve)),
    );
    const op = operator([0], AGGREGATES, sink, { signal: controller.signal, now: () => 0 });
    for (let from = 0; from < 10_000; from += 2048)
      op.push(batch(from, Math.min(2048, 10_000 - from), 10_000, (r) => BigInt(r)));
    const done = op.finish();
    expect(pushes()).toBe(1);
    controller.abort(reason);
    release();
    await expect(done).rejects.toBe(reason);
    expect(pushes()).toBe(1);
  });

  it("yields a task even when every output batch meets backpressure that is ready at once", async () => {
    // Review B4-p2c-4 finding 1: one group per batch, each push "blocked" with a ready() already
    // resolved (a microtask), 1 ms of downstream work per push. The slice must still end at
    // 4 ms and yield a task, so a queued task runs long before the 200 batches are done.
    let tick = 0;
    let pushes = 0;
    let pushesBeforeTask = -1;
    const sink: Operator = {
      push: () => {
        pushes += 1;
        tick += 1;
        return "blocked";
      },
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    const op = operator([0], [call("count_star", I64, null)], sink, {
      now: () => tick,
      targetBytes: 1,
    });
    op.push(batch(0, 200, 200, (r) => BigInt(r)));
    setImmediate(() => {
      pushesBeforeTask = pushes;
    });
    await op.finish();
    expect(pushes).toBe(200);
    expect(pushesBeforeTask).toBeGreaterThanOrEqual(0);
    expect(pushesBeforeTask).toBeLessThanOrEqual(8);
  });

  it("two interleaved instances (yielding, NUMERIC limbs, DISTINCT) give each one's answer", async () => {
    const run = async (interleave: boolean) => {
      let tickA = 0;
      let tickB = 0;
      const a = collector();
      const b = collector();
      // Small output batches and slices: many yields between the two instances' batches (AVG
      // prepares at most what each batch holds, charged and per instance; per-instance state such
      // as prepared results, output charges and the cursor must stay apart).
      const opA = operator([0], AGGREGATES, a.sink, { now: () => tickA++, targetBytes: 8192 });
      const opB = operator([0], AGGREGATES, b.sink, {
        now: () => (tickB += 3),
        targetBytes: 8192,
      });
      const valueA = (r: number) => BigInt(r) * 7n * 10n ** 33n + 1n;
      const valueB = (r: number) => -BigInt(r) * 3n * 10n ** 32n - 5n;
      for (let from = 0; from < 12_000; from += 1000) {
        opA.push(batch(from, 1000, 3000, valueA));
        if (interleave) opB.push(batch(from, 1000, 2500, valueB));
      }
      if (!interleave)
        for (let from = 0; from < 12_000; from += 1000) opB.push(batch(from, 1000, 2500, valueB));
      if (interleave) await Promise.all([opA.finish(), opB.finish()]);
      else {
        await opA.finish();
        await opB.finish();
      }
      return [a.rows, b.rows];
    };
    const [aloneA, aloneB] = await run(false);
    const [togetherA, togetherB] = await run(true);
    expect(togetherA).toEqual(aloneA);
    expect(togetherB).toEqual(aloneB);
    expect(aloneA?.length).toBe(3000);
  }, 120_000);
});
