import { expect, it } from "vitest";
import type { Snapshot } from "../access/snapshot.js";
import type { PhysicalPlan } from "../ir/ir.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { NumericOutOfRangeError, PlanStaleError, QueryCancelledError } from "./errors.js";
import type { ExecuteRequest } from "./executor.js";
import { I32, numbers } from "./kernels/test-support.js";
import { QueryExecution, type Program } from "./run.js";
import { WindowError } from "./window-frames.js";
import type { ChunkReads } from "./scan.js";

function input(options: { signal?: AbortSignal; windowBytes?: number } = {}) {
  const catalog = { version: 0 };
  const plan: PhysicalPlan = {
    root: { id: 1, op: "values", rows: [], output: [{ name: "v", type: I32, nullable: false }] },
    columns: [{ name: "v", type: I32, nullable: false }],
    parameters: [],
    placeholderCount: 0,
    clockSlot: null,
    initPlans: [],
    catalogVersion: catalog.version,
    volatile: false,
  };
  const governor = new Governor(8 * 1024 * 1024);
  // Values-only lifecycle tests do not read tables. The end-to-end adapter uses real snapshots.
  const snapshot = { catalog, observedLsn: 17 } as unknown as Snapshot;
  const request: ExecuteRequest = {
    snapshot,
    statement: null,
    parameters: { slots: [] },
    statementTime: 0n,
    reads: {} as ChunkReads,
    governor,
    spill: new MemorySpillService(),
    queryId: 1,
    sequences: null,
    sequenceSession: null,
    returning: "atExecuted",
    resultWindowBytes: options.windowBytes ?? 64,
    ...(options.signal === undefined ? {} : { signal: options.signal }),
  };
  return { plan, request, governor };
}

it("preserves a window operator's PostgreSQL SQLSTATE at the execution boundary", async () => {
  const h = input();
  const execution = new QueryExecution(h.plan, h.request, () => {
    throw new WindowError("frame offset must not be null", "22004");
  });
  await expect(execution.executed).rejects.toMatchObject({
    code: "MINNOW_WINDOW_ERROR",
    sqlState: "22004",
  });
  await expect(execution.delivered).rejects.toMatchObject({ sqlState: "22004" });
  expect(h.governor.usedBytes).toBe(0);
});

it("finishes a synchronous producer inside execute and keeps only frame memory until credit", async () => {
  const h = input();
  let cleanup = false;
  const execution = new QueryExecution(h.plan, h.request, (runtime, sink) => {
    runtime.pipeline();
    const grant = runtime.admit("values", 4096, false, h.plan.root);
    if (grant instanceof Promise) throw new Error("unexpected admission wait");
    expect(grant.allocate(32)).toBe(true);
    runtime.dispose(() => {
      grant.free(32);
      cleanup = true;
    });
    expect(sink.push({ length: 3, columns: [numbers(I32, [1, 2, 3])], selection: null })).toBe(
      "ready",
    );
    return sink.finish();
  });
  expect(execution.state).toBe("executed");
  expect(cleanup).toBe(true);
  expect(execution.summary?.rowCount).toBe(3);
  expect(execution.summary?.profile.synchronous).toBe(true);
  expect(execution.summary?.profile.pipelines).toBe(1);
  const frame = execution.results.poll();
  expect(frame?.requiresLsn).toBe(17);
  expect(h.governor.usedBytes).toBeGreaterThan(0);
  frame?.release();
  await execution.delivered;
  expect(h.governor.usedBytes).toBe(0);
});

it("refuses a stale plan before admission or program activation", async () => {
  const h = input();
  let ran = false;
  const execution = new QueryExecution(
    { ...h.plan, catalogVersion: h.plan.catalogVersion + 1 },
    h.request,
    () => {
      ran = true;
    },
  );
  expect(execution.state).toBe("failed");
  await expect(execution.executed).rejects.toBeInstanceOf(PlanStaleError);
  await expect(execution.delivered).rejects.toBeInstanceOf(PlanStaleError);
  expect(ran).toBe(false);
  expect(h.governor.peakBytes).toBe(0);
});

it("cancels a queued admission without starting the program", async () => {
  const h = input();
  const blocker = await h.governor.admit({
    label: "other",
    minimumBytes: h.governor.capBytes - h.governor.reserveBytes,
    spillable: false,
  });
  let ran = false;
  const execution = new QueryExecution(h.plan, h.request, () => {
    ran = true;
  });
  expect(execution.state).toBe("running");
  execution.cancel("waiting");
  await expect(execution.executed).rejects.toBeInstanceOf(QueryCancelledError);
  await expect(execution.delivered).rejects.toBeInstanceOf(QueryCancelledError);
  expect(ran).toBe(false);
  blocker.close();
  expect(h.governor.usedBytes).toBe(0);
});

it("preserves the first SQL error while aborting every resource", async () => {
  const h = input();
  const failure = new NumericOutOfRangeError("first failure");
  let lastAbort = false;
  const execution = new QueryExecution(h.plan, h.request, (runtime) => {
    const grant = runtime.admit("producer", 4096, false);
    if (grant instanceof Promise) throw new Error("unexpected wait");
    grant.allocate(99);
    runtime.onAbort(() => {
      throw new Error("secondary cleanup failure");
    });
    runtime.onAbort(() => {
      lastAbort = true;
    });
    throw failure;
  });
  expect(execution.state).toBe("failed");
  await expect(execution.executed).rejects.toBe(failure);
  await expect(execution.delivered).rejects.toBe(failure);
  expect(lastAbort).toBe(true);
  expect(h.governor.usedBytes).toBe(0);
});

it("does not end result delivery before asynchronous pipeline cleanup succeeds", async () => {
  const h = input();
  const failure = new Error("late cleanup failure");
  const execution = new QueryExecution(h.plan, h.request, (runtime, sink) => {
    runtime.dispose(() => Promise.reject(failure));
    return sink.finish();
  });
  await expect(execution.executed).rejects.toBe(failure);
  await expect(execution.delivered).rejects.toBe(failure);
  expect(execution.state).toBe("failed");
  expect(h.governor.usedBytes).toBe(0);
});

it("cancellation during credit backpressure closes pipelines and handed-out frames", async () => {
  const h = input();
  let released = 0;
  const program: Program = async (runtime, sink) => {
    const grant = await runtime.admit("producer", 4096, false);
    grant.allocate(40);
    if (
      sink.push(
        {
          length: 100,
          columns: [
            numbers(
              I32,
              Array.from({ length: 100 }, (_, i) => i),
            ),
          ],
          selection: null,
        },
        {
          release: () => {
            released++;
          },
        },
      ) === "blocked"
    )
      await sink.ready();
    await sink.finish();
  };
  const execution = new QueryExecution(h.plan, h.request, program);
  const frame = await execution.results.next();
  execution.cancel("credit blocked");
  await expect(execution.executed).rejects.toBeInstanceOf(QueryCancelledError);
  await expect(execution.delivered).rejects.toBeInstanceOf(QueryCancelledError);
  expect(execution.state).toBe("cancelled");
  expect(released).toBe(1);
  expect(h.governor.usedBytes).toBe(0);
  frame?.release();
  expect(h.governor.usedBytes).toBe(0);
});

it("after executed, cancellation discards results without aborting completed pipelines", async () => {
  const h = input();
  let aborted = 0;
  const execution = new QueryExecution(h.plan, h.request, (runtime, sink) => {
    runtime.onAbort(() => {
      aborted++;
    });
    sink.push({ length: 1, columns: [numbers(I32, [7])], selection: null });
    return sink.finish();
  });
  const summary = await execution.executed;
  const frame = execution.results.poll();
  execution.cancel("unused result");
  await expect(execution.delivered).rejects.toBeInstanceOf(QueryCancelledError);
  expect(execution.state).toBe("executed");
  expect(execution.summary).toBe(summary);
  expect(aborted).toBe(0);
  expect(h.governor.usedBytes).toBe(0);
  frame?.release();
});
