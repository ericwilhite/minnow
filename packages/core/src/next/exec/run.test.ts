import { DeferredGrant } from "./deferred-grant.js";
import { ResultSink } from "./result-sink.js";
import { ResultChannel } from "./result.js";
import { acquirePhasePool } from "./phase-pool.js";
import { expect, it } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { MAX_BLOB_BYTES } from "../access/space.js";
import type { Snapshot } from "../access/snapshot.js";
import type { PhysicalPlan } from "../ir/ir.js";
import { Governor, type Grant } from "../memory/governor.js";
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

it.each(seedsFor("next-executor-explicit-loans", [0xb4cd, 0xb4ce]))(
  "admits only explicit project/filter loan owners, including their helpers (seed %i)",
  async (seed) => {
    const reach = createReach("explicit expression loans", [
      "project",
      "filter",
      "ordinary unchanged",
      "private base",
      "private helpers",
      "queued competitor",
      "no global growth",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("project" as const, "filter" as const),
        fc.integer({ min: 1, max: 2 }),
        fc.integer({ min: 64, max: 4096 }),
        async (op, slots, bytes) => {
          reach.beginHistory();
          const h = input(),
            controller = new AbortController(),
            runtime = new QueryRuntime(h.plan, h.request, h.request.snapshot, controller.signal),
            binding = new DeferredGrant("explicit expression result"),
            rel = { id: 9, op };
          runtime.atomicResult(binding, 64);
          const ordinary = runtime.minimumBytes([{ rel, minimum: 64 }]);
          expect(ordinary).toBe(64 + MAX_BLOB_BYTES);
          const entries = [{ rel, minimum: 64, loanSlots: slots }];
          expect(runtime.minimumBytes(entries) - ordinary).toBe((slots + 1) * MAX_BLOB_BYTES);
          await runtime.reserve(entries);
          const base = await runtime.admit(`${op}#9`, 64, true, rel);
          expect(base.held).toBe(64 + MAX_BLOB_BYTES);
          expect(base.allocate(64)).toBe(true);
          const helpers = Array.from({ length: slots }, (_, slot) => runtime.loan(rel, slot));
          for (const helper of helpers) expect(helper.held).toBe(MAX_BLOB_BYTES);
          let admitted = false;
          const blocked = h.governor
            .admit({
              label: "queued expression competitor",
              minimumBytes:
                h.governor.capBytes - h.governor.reserveBytes - h.governor.usedBytes + 1,
              spillable: false,
              signal: controller.signal,
            })
            .then(
              (grant) => {
                admitted = true;
                grant.close();
              },
              () => undefined,
            );
          const before = h.governor.usedBytes;
          try {
            const owners = [base, ...helpers];
            const loans = await Promise.all(
              owners.map((owner) => owner.borrow(bytes, controller.signal)),
            );
            for (const owner of owners) expect(owner.allocate(bytes)).toBe(true);
            expect(admitted).toBe(false);
            expect(h.governor.usedBytes).toBe(before);
            for (const owner of owners) owner.free(bytes);
            for (const loan of loans) loan.repay();
            base.free(64);
            reach.reach(op);
            reach.reach("ordinary unchanged");
            reach.reach("private base");
            reach.reach("private helpers");
            reach.reach("queued competitor");
            reach.reach("no global growth");
          } finally {
            controller.abort(new Error("end explicit expression check"));
            await blocked;
            await runtime.close();
            binding.close();
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("released");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          ["project", 1, 64],
          ["filter", 2, 4096],
        ],
      },
    );
    reach.assertReached();
  },
);

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

it.each(seedsFor("producer-phase-retirement", [0xb4c7, 0xb4c8]))(
  "drains nested producer resources before returning native credit (seed %i)",
  async (seed) => {
    const reach = createReach("producer retirement", [
      "nested",
      "drained before release",
      "once",
      "native credit returned",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 5 }), async (depth) => {
        reach.beginHistory();
        const h = input(),
          runtime = new QueryRuntime(
            h.plan,
            h.request,
            h.request.snapshot,
            new AbortController().signal,
          );
        runtime.enablePhases();
        const events: string[] = [];
        const nest = (n: number): Promise<void> => {
          let grant: Grant | undefined;
          return Promise.resolve(
            runtime.resource(
              async () => {
                grant = await h.governor.admit({
                  label: `producer ${String(n)}`,
                  minimumBytes: 4096,
                  spillable: false,
                });
                expect(grant.allocate(64)).toBe(true);
                runtime.onAbort(() => events.push(`abort ${String(n)}`));
                runtime.dispose(async () => {
                  await Promise.resolve();
                  grant?.free(64);
                  events.push(`drain ${String(n)}`);
                });
                if (n > 0) await nest(n - 1);
                else runtime.beginHandoff();
              },
              () => {
                events.push(`release ${String(n)}`);
                grant?.close();
              },
            ),
          );
        };
        await nest(depth);
        for (let n = 0; n <= depth; n++)
          expect(events.slice(n * 3, n * 3 + 3)).toEqual([
            `abort ${String(n)}`,
            `drain ${String(n)}`,
            `release ${String(n)}`,
          ]);
        const saved = [...events];
        await runtime.close();
        expect(events).toEqual(saved);
        expect(h.governor.usedBytes).toBe(0);
        reach.reach("nested");
        reach.reach("drained before release");
        reach.reach("once");
        reach.reach("native credit returned");
      }),
      { seed, numRuns: 6 },
    );
    reach.assertReached();
  },
);

it("retires production native parts and atomically binds the remaining plan on the same pin", async () => {
  const h = input(),
    signal = new AbortController().signal,
    runtime = new QueryRuntime(h.plan, h.request, h.request.snapshot, signal),
    binding = new DeferredGrant("result phase"),
    channel = new ResultChannel(h.plan.columns, { windowBytes: 64 }),
    sink = new ResultSink([I32], binding, channel, 17);
  runtime.atomicResult(binding, 64);
  runtime.enablePhases();
  await runtime.reserve([{ rel: h.plan.root, minimum: 4096 }]);
  const grant = await runtime.admit("values#1", 4096, true, h.plan.root);
  await runtime.resource(
    () => {
      runtime.beginHandoff();
    },
    () => grant.close(),
  );
  const lease = await acquirePhasePool(h.governor, signal);
  try {
    await runtime.suspendProducer(sink, lease.pool);
    expect(runtime.params.slots).toHaveLength(0);
    expect(binding.held).toBe(0);
    expect(runtime.view).toBe(h.request.snapshot);
    await runtime.restorePhase(
      [{ rel: h.plan.root, minimum: 4096 }],
      { bytes: 0, largestSlot: 0, bind: () => ({ slots: [] }) },
      lease.pool,
    );
    sink.resumeBudget();
    expect(runtime.handingOff).toBe(false);
    if (sink.push({ length: 1, columns: [numbers(I32, [123])], selection: null }) === "blocked")
      await sink.ready();
    const frame = channel.poll();
    if (!frame) throw new Error("missing restored output");
    expect(frame.payload.rowCount).toBe(1);
    frame.release();
    await sink.finish();
    await channel.delivered;
    await runtime.close();
    binding.close();
  } finally {
    lease.release();
    sink.discard(new Error("cleanup"));
    binding.close();
    await runtime.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

it.each(seedsFor("next-phase-progress-envelope", [0xb4d9, 0xb4da]))(
  "carries the complete saved step without multiplying it again after producer retirement (seed %i)",
  async (seed) => {
    const reach = createReach("phase progress envelope", [
      "compound initial",
      "saved unchanged",
      "future source",
      "zero",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 5 }),
        fc.integer({ min: MAX_BLOB_BYTES, max: 2 * MAX_BLOB_BYTES }),
        async (multiple, source) => {
          reach.beginHistory();
          const h = input(),
            runtime = new QueryRuntime(
              h.plan,
              h.request,
              h.request.snapshot,
              new AbortController().signal,
            );
          const result = new DeferredGrant("phase result");
          runtime.atomicResult(result, 64);
          runtime.progressEnvelope(65536, multiple);
          runtime.readMinimum(0, source);
          const entries = [{ rel: { id: 9, op: "aggregate" as const }, minimum: 64, loanSlots: 1 }];
          const progress = source * multiple + 65536;
          expect(runtime.minimumBytes(entries)).toBe(64 + 3 * progress);
          reach.reach("compound initial");
          runtime.readMinimum(0, 0, progress);
          expect(runtime.minimumBytes(entries)).toBe(64 + 3 * progress);
          reach.reach("saved unchanged");
          runtime.readMinimum(0, 2 * source, progress);
          expect(runtime.minimumBytes(entries)).toBe(64 + 3 * (2 * source * multiple + 65536));
          reach.reach("future source");
          await runtime.close();
          result.close();
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("zero");
        },
      ),
      { seed, numRuns: 10 },
    );
    reach.assertReached();
  },
);
