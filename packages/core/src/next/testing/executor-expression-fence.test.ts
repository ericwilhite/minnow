/** A cancelled internal key projection must retire its borrow before releasing compiler credit. */
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, EXPRESSION_FENCE_REACH } from "../../testing/reach.js";
import { QueryRuntime } from "../exec/run.js";
import type { Grant } from "../memory/governor.js";
import { Governor } from "../memory/governor.js";
import { Slice } from "../exec/slices.js";
import * as pipeline from "../exec/pipeline.js";
import { StorageCore } from "./storage-core.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver retained
const due = Slice.prototype.due;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver retained
const admit = QueryRuntime.prototype.admit;
const yieldTask = pipeline.yieldTask;
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("executor-expression-fence", [0xb7bb, 0xb7bc]))(
  "fences cancelled internal expression helpers seed%i",
  async (seed) => {
    const core = await new StorageCore().open();
    const reach = createReach("expression helper fence", EXPRESSION_FENCE_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(fc.boolean(), fc.integer({ min: 3, max: 8 }), async (join, n) => {
          reach.beginHistory();
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
          let suspended = false;
          let keyGrant: Grant | undefined;
          const admitMock = vi.spyOn(QueryRuntime.prototype, "admit").mockImplementation(function (
            this: QueryRuntime,
            ...args
          ) {
            const grant = admit.apply(this, args);
            const record = (value: Grant) => {
              if (args[0].startsWith(join ? "join#" : "sort#")) keyGrant = value;
              return value;
            };
            return pipeline.isPromise(grant) ? grant.then(record) : record(grant);
          });
          const inReplay = () => new Error().stack?.includes("precise") === true;
          const dueMock = vi.spyOn(Slice.prototype, "due").mockImplementation(function (
            this: Slice,
          ) {
            return inReplay() || due.call(this);
          });
          const yieldMock = vi.spyOn(pipeline, "yieldTask").mockImplementation(() => {
            if (inReplay() && !suspended) {
              suspended = true;
              enter();
              return paused;
            }
            return yieldTask();
          });
          const prepareMock = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
            const q = prepare(plan);
            return {
              ...q,
              execute: (request) =>
                q.execute({ ...request, governor, signal: AbortSignal.timeout(30000) }),
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
          const deadline = setTimeout(() => {
            enter();
            run.execution.cancel(new Error("replay entry deadline"));
          }, 10000);
          try {
            await entered;
            expect(suspended).toBe(true);
            run.execution.cancel();
            await new Promise((resolve) => setTimeout(resolve, 20));
            expect(
              settled,
              "execution must retain compiler resources until the helper exits its yield",
            ).toBe(false);
            expect(governor.usedBytes).toBeGreaterThan(0);
            expect(
              keyGrant?.held,
              "native breaker must not retire its helper's compiler grant",
            ).toBeGreaterThan(0);
            resume();
            await completed;
            reach.reach(join ? "join key" : "sort key");
            reach.reach("cancelled suspension");
          } finally {
            clearTimeout(deadline);
            resume();
            run.execution.cancel();
            await completed;
            run.release();
            prepareMock.mockRestore();
            dueMock.mockRestore();
            yieldMock.mockRestore();
            admitMock.mockRestore();
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("cleanup zero");
          }
        }),
        {
          seed,
          numRuns: 4,
          endOnFailure: true,
          examples: [
            [true, 3],
            [false, 3],
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
