/** A cancelled internal key projection must retire its borrow before releasing compiler credit. */
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, EXPRESSION_FENCE_REACH } from "../../testing/reach.js";
import * as filter from "../exec/filter.js";
import { QueryRuntime } from "../exec/run.js";
import type { Grant } from "../memory/governor.js";
import { Governor } from "../memory/governor.js";
import { Slice } from "../exec/slices.js";
import * as pipeline from "../exec/pipeline.js";
import { StorageCore } from "./storage-core.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver retained
const admit = QueryRuntime.prototype.admit;
const ProjectOperator = filter.ProjectOperator;
// eslint-disable-next-line @typescript-eslint/unbound-method -- getter called with its original receiver
const closed = Object.getOwnPropertyDescriptor(ProjectOperator.prototype, "closed")?.get as (
  this: filter.ProjectOperator,
) => void | Promise<void>;
const originalStackLimit = Error.stackTraceLimit;
afterEach(() => {
  vi.restoreAllMocks();
  Error.stackTraceLimit = originalStackLimit;
});
it.each(seedsFor("executor-expression-fence", [0xb7bb, 0xb7bc]))(
  "fences cancelled internal expression helpers seed%i",
  async (seed) => {
    const core = await new StorageCore().open();
    const reach = createReach("expression helper fence", EXPRESSION_FENCE_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.integer({ min: 3, max: 8 }),
          fc.constantFrom(1, 10),
          async (join, n, traceLimit) => {
            reach.beginHistory();
            const previousLimit = Error.stackTraceLimit;
            Error.stackTraceLimit = traceLimit;
            const governor = new Governor(64 << 20);
            const prepare = core.executor.prepare.bind(core.executor);
            let enter: () => void = () => undefined,
              resume: () => void = () => undefined;
            const entered = new Promise<void>((resolve) => {
              enter = resolve;
            });
            const paused = new Promise<void>((resolve) => {
              resume = resolve;
            });
            let fenceRequested: () => void = () => undefined;
            const closing = new Promise<void>((resolve) => {
              fenceRequested = resolve;
            });
            let helper: filter.ProjectOperator | undefined;
            let helperFence: Promise<void> | undefined;
            let pushes = 0;
            const restores: Array<() => void> = [];
            const controller = new AbortController();
            let keyGrant: Grant | undefined;
            const admitMock = vi
              .spyOn(QueryRuntime.prototype, "admit")
              .mockImplementation(function (this: QueryRuntime, ...args) {
                const grant = admit.apply(this, args);
                const record = (value: Grant) => {
                  if (args[0].startsWith(join ? "join#" : "sort#")) keyGrant = value;
                  return value;
                };
                return pipeline.isPromise(grant) ? grant.then(record) : record(grant);
              });
            // The error must share a batch with earlier valid rows. An elapsed source
            // slice can instead flush one-row batches: replay then throws on its first
            // row, never reaching the suspension this law needs. Freeze slicing and
            // suspend via explicit consumer credit, independently of stack names/time.
            const dueMock = vi.spyOn(Slice.prototype, "due").mockReturnValue(false);
            const projectMock = vi.spyOn(filter, "ProjectOperator").mockImplementation(function (
              ...args: ConstructorParameters<typeof ProjectOperator>
            ) {
              const project = new ProjectOperator(...args);
              if (args[0].some((expr) => expr.op !== "column")) {
                const downstream = project.downstream;
                const push = downstream.push.bind(downstream);
                const ready = downstream.ready.bind(downstream);
                const pushMock = vi
                  .spyOn(downstream, "push")
                  .mockImplementation((batch, transfer) => {
                    // A full-batch data error delivers nothing. A one-row selection on
                    // the original batch proves this is its precise replay continuation.
                    expect(batch.length).toBe(n);
                    expect(batch.selection?.count).toBe(1);
                    pushes++;
                    push(batch, transfer);
                    return "blocked";
                  });
                const readyMock = vi.spyOn(downstream, "ready").mockImplementation(async () => {
                  // Let native work settle before holding the helper's borrow. Abort
                  // cannot reject that work and bypass our controlled continuation.
                  await ready();
                  helper = project;
                  enter();
                  await paused;
                });
                restores.push(
                  () => pushMock.mockRestore(),
                  () => readyMock.mockRestore(),
                );
              }
              return project;
            });
            const closedMock = vi
              .spyOn(ProjectOperator.prototype, "closed", "get")
              .mockImplementation(function (this: filter.ProjectOperator) {
                const pending = closed.call(this);
                if (this === helper) {
                  expect(pipeline.isPromise(pending)).toBe(true);
                  if (pipeline.isPromise(pending)) helperFence = pending;
                  fenceRequested();
                }
                return pending;
              });
            const prepareMock = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
              const q = prepare(plan);
              return {
                ...q,
                execute: (request) =>
                  q.execute({ ...request, governor, signal: controller.signal }),
              };
            });
            const sql = join
              ? `SELECT l.i FROM generate_series(1,${String(n)})l(i) JOIN generate_series(1,${String(n)})r(i) ON l.i=10/(${String(n)}-r.i)`
              : `SELECT i FROM generate_series(1,${String(n)})g(i) ORDER BY 10/(${String(n)}-i)`;
            const run = core.start(core.prepare(sql));
            let settled = false;
            const completed = Promise.allSettled([
              run.execution.executed,
              run.execution.delivered,
            ]).then(() => {
              settled = true;
            });
            const unexpectedSettlement = completed.then(() => {
              throw new Error("execution settled before its controlled helper fence");
            });
            try {
              await Promise.race([entered, unexpectedSettlement]);
              expect(helper).toBeDefined();
              const pushesBeforeCancel = pushes;
              run.execution.cancel();
              await Promise.race([closing, unexpectedSettlement]);
              expect(
                settled,
                "execution must retain compiler resources until the helper exits its borrow",
              ).toBe(false);
              expect(governor.usedBytes).toBeGreaterThan(0);
              expect(
                keyGrant?.held,
                "native breaker must not retire its helper's compiler grant",
              ).toBeGreaterThan(0);
              reach.reach("helper close fence");
              resume();
              await helperFence;
              await completed;
              expect(pushes, "a cancelled helper must not deliver another row").toBe(
                pushesBeforeCancel,
              );
              reach.reach("no post-cancel rows");
              reach.reach(join ? "join key" : "sort key");
              reach.reach("cancelled suspension");
              if (traceLimit === 1) reach.reach("shallow ambient stack");
            } finally {
              Error.stackTraceLimit = previousLimit;
              controller.abort();
              resume();
              run.execution.cancel();
              await completed;
              run.release();
              prepareMock.mockRestore();
              dueMock.mockRestore();
              closedMock.mockRestore();
              projectMock.mockRestore();
              for (const restore of restores) restore();
              admitMock.mockRestore();
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("cleanup zero");
            }
          },
        ),
        {
          seed,
          numRuns: 4,
          endOnFailure: true,
          examples: [
            [true, 3, 1],
            [false, 3, 10],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
  },
  60000,
);
