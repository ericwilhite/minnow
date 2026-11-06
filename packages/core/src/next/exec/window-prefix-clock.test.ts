/** R23.2/R23.5: prefix building and frame scans consult the task clock within bounded chunks. */
import { expect, it, vi } from "vitest";
import { aggregate, type AggregateKind, type Peers } from "./window-frames.js";
import { TaskClock, taskClock, drive } from "./pipeline.js";
import * as pipeline from "./pipeline.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ArraySource, CollectSink, batchesOf, INT, DBL, type Row } from "./join-fixtures.js";
import { WindowOperator, windowMinimumBytes, type WindowSpec } from "./window.js";
import { Slice, runSliced } from "./slices.js";

const n = 32768;
function probe(mode: string) {
  let reads = 0;
  const watch = <T extends Float64Array | Uint8Array | bigint[]>(array: T): T =>
    new Proxy(array, {
      get(target, key) {
        if (typeof key === "string" && /^\d+$/.test(key)) reads++;
        return Reflect.get(target, key, target) as unknown;
      },
    });
  const skip = mode === "promotion" ? n : 0;
  const clock = new TaskClock(() => Math.max(0, reads - skip) / 1000);
  const slice = new Slice(4, clock);
  const counts = mode === "counts";
  const moving = mode === "moving sum" || mode === "moving min";
  const kind: AggregateKind = counts ? "count" : mode.endsWith("min") ? "min" : "sum";
  const exact = mode === "big prefix" || mode === "promotion" || mode === "big min";
  const values = new Float64Array(n).fill(mode === "promotion" ? 2 ** 52 : 1);
  const valid = new Uint8Array(n).fill(1);
  const big = new Array<bigint>(n).fill((1n << 62n) + 1n);
  const peers: Peers = {
    n,
    start: new Int32Array(n),
    end: new Int32Array(n).fill(n - 1),
    group: new Int32Array(n),
    groupStart: new Int32Array(n),
    groupEnd: new Int32Array(n).fill(n - 1),
    groups: 1,
  };
  const job = aggregate(
    kind,
    {
      values:
        mode === "ordinary prefix" || mode === "promotion" || moving || mode === "running min"
          ? watch(values)
          : values,
      valid: counts ? watch(valid) : valid,
      big: mode === "big prefix" || mode === "big min" ? watch(big) : null,
    },
    exact,
    peers,
    "no-others",
    new Int32Array(n).fill(moving ? 1 : 0),
    new Int32Array(n).fill(n - 1),
    { values: new Float64Array(n), valid: new Uint8Array(n), big: null },
    slice,
  );
  return { job, slice, reads: () => Math.max(0, reads - skip) };
}
for (const mode of [
  "counts",
  "ordinary prefix",
  "big prefix",
  "promotion",
  "moving sum",
  "running min",
  "moving min",
  "big min",
])
  it(`${mode} suspends within the 4 ms deadline plus one bounded chunk`, () => {
    const p = probe(mode);
    expect(p.job.next().done).toBe(false);
    expect(p.reads()).toBeGreaterThanOrEqual(4000);
    expect(p.reads()).toBeLessThanOrEqual(5120);
    p.job.return();
  });
it("a prefix resumption observes abort before doing further prefix work", async () => {
  const p = probe("big prefix");
  const controller = new AbortController();
  const reason = new Error("cancel prefix");
  let stopped = 0;
  const run = runSliced(p.job, p.slice, () => {
    stopped = p.reads();
    controller.abort(reason);
    throw controller.signal.reason;
  });
  await expect(run).rejects.toBe(reason);
  expect(p.reads()).toBe(stopped);
  p.job.return();
});

it("streamed moving-frame scans bound modeled comparison work between genuine task yields", async () => {
  const count = 9216;
  const spec: WindowSpec = {
    types: [INT, DBL],
    partitionBy: [],
    orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
    streamRows: 1,
    functions: [
      {
        fn: "min",
        arg: 1,
        type: DBL,
        frame: {
          unit: "rows",
          start: { kind: "preceding", offset: 8192 },
          end: { kind: "current-row", offset: null },
          exclude: "no-others",
        },
      },
    ],
  };
  const data: Row[] = Array.from({ length: count }, (_, i) => [i, i]);
  const input = batchesOf(spec.types, data);
  const governor = new Governor(64 << 20);
  const grant = await governor.admit({
    label: "clock",
    minimumBytes: windowMinimumBytes(spec.types),
    spillable: true,
  });
  const spill = new MemorySpillService();
  const sink = new CollectSink();
  const op = new WindowOperator(spec, { grant, spill, queryId: 1 }, sink);
  const now = taskClock.now;
  const isNaN = Number.isNaN;
  const yieldReal = pipeline.yieldTask;
  let reads = 0,
    work = 0,
    peak = 0;
  const yielded = vi.spyOn(pipeline, "yieldTask").mockImplementation(async () => {
    await yieldReal();
    work = 0;
  });
  Number.isNaN = (value) => {
    reads++;
    work++;
    peak = Math.max(peak, work);
    return isNaN(value);
  };
  taskClock.now = () => reads / 1000;
  taskClock.begin();
  try {
    await drive(new ArraySource(input), op);
    await op.closed;
    expect(peak).toBeGreaterThanOrEqual(4000);
    expect(peak).toBeLessThanOrEqual(6144);
    expect(yielded).toHaveBeenCalled();
    expect(sink.rows.at(-1)?.[2]).toBe(count - 1 - 8192);
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  } finally {
    Number.isNaN = isNaN;
    yielded.mockRestore();
    taskClock.now = now;
    taskClock.begin();
    op.abort(new Error("clock test cleanup"));
    await op.closed;
  }
}, 120000);
