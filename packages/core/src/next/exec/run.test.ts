import { expect, it } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import type { Snapshot } from "../access/snapshot.js";
import type { PhysicalPlan } from "../ir/ir.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { NumericOutOfRangeError, PlanStaleError, QueryCancelledError } from "./errors.js";
import type { ExecuteRequest } from "./executor.js";
import { I32, numbers } from "./kernels/test-support.js";
import { QueryExecution, QueryRuntime, type Program } from "./run.js";
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
    parameters: [],
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

it("reserves nested loan owners before fixed buffers and keeps them independent behind a queued admission", async () => {
  const h = input();
  const runtime = new QueryRuntime(
    h.plan,
    h.request,
    h.request.snapshot,
    new AbortController().signal,
  );
  const normal = h.governor.capBytes - h.governor.reserveBytes;
  const controller = new AbortController();
  const stop = new Error("end blocked admission");
  let admitted = false;
  await runtime.reserve([{ rel: h.plan.root, minimum: normal, loanSlots: 2 }]);
  const base = runtime.admit("values#1", normal, true, h.plan.root);
  expect(base).not.toBeInstanceOf(Promise);
  const blocked = h.governor
    .admit({ label: "other pipeline", minimumBytes: 1, spillable: true, signal: controller.signal })
    .then(
      (grant) => {
        admitted = true;
        grant.close();
      },
      () => undefined,
    );
  try {
    const first = runtime.loan(h.plan.root, 0),
      second = runtime.loan(h.plan.root, 1);
    // A late zero-byte admission would wait behind the blocked pipeline. Reject that design
    // synchronously, not through a timeout, and consume any mutant's eventual rejection.
    if (first instanceof Promise) void first.catch(() => undefined);
    if (second instanceof Promise) void second.catch(() => undefined);
    expect(first).not.toBeInstanceOf(Promise);
    expect(second).not.toBeInstanceOf(Promise);
    expect(first).not.toBe(second);
    const a = await first.borrow(128),
      b = await second.borrow(256);
    expect(first.allocate(128)).toBe(true);
    expect(second.allocate(256)).toBe(true);
    expect([first.loaned, second.loaned]).toEqual([128, 256]);
    expect(runtime.profile(h.plan.root).peakHeldBytes).toBe(normal + 384);
    expect(admitted).toBe(false);
    first.free(128);
    second.free(256);
    a.repay();
    b.repay();
  } finally {
    controller.abort(stop);
    await blocked;
    await runtime.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

it.each(seedsFor("next-executor-helper-profile", [0xd10b, 0xd10c]))(
  "attributes simultaneous helper holdings to the plan operator (seed %i)",
  async (seed) => {
    const reach = createReach("helper profiles", [
      "simultaneous",
      "non-overlapping",
      "logical-id",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 64, max: 4096 }),
        fc.integer({ min: 64, max: 4096 }),
        fc.boolean(),
        async (first, second, overlap) => {
          reach.beginHistory();
          const h = input();
          const execution = new QueryExecution(h.plan, h.request, async (runtime, sink) => {
            const original = await runtime.admit("logical", first, true, h.plan.root);
            if (!overlap) original.close();
            await runtime.admit("helper", second, true, {
              id: 999,
              op: "sort",
              profileAs: h.plan.root,
            });
            const profiles = runtime.summary(false).operators;
            expect(profiles).toHaveLength(1);
            expect(profiles[0]?.relId).toBe(h.plan.root.id);
            expect(profiles[0]?.op).toBe(h.plan.root.op);
            expect(profiles[0]?.peakHeldBytes).toBe(
              overlap ? first + second : Math.max(first, second),
            );
            return sink.finish();
          });
          await execution.executed;
          await execution.delivered;
          expect(h.governor.usedBytes).toBe(0);
          reach.reach(overlap ? "simultaneous" : "non-overlapping");
          reach.reach("logical-id");
          reach.reach("released");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [64, 128, true],
          [128, 64, false],
        ],
      },
    );
    reach.assertReached();
  },
);

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
