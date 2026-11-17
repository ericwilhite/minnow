import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, EXECUTION_RETIREMENT_REACH } from "../../testing/reach.js";
import type { Snapshot } from "../access/snapshot.js";
import type { PhysicalPlan } from "../ir/ir.js";
import { beginTask, taskClock } from "../task.js";
import { Governor } from "../memory/governor.js";
import { boundSource } from "../page/read-bounds.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { ExecuteRequest, Execution } from "./executor.js";
import { QueryExecution, type Program } from "./run.js";
import { abortable } from "./pipeline.js";
import type { ChunkReads } from "./scan.js";

const INT = { kind: "int32", bits: 32 } as const;
const column = { name: "v", type: INT, nullable: false };
function resourceFence(execution: Execution): Promise<void> {
  return (execution.settled?.() ?? execution.delivered).catch(() => undefined);
}

it.each(seedsFor("next-execution-retirement", [8850, 8851]))(
  "fences generated cancel/dispose during native sizing seed=%i",
  async (seed) => {
    const reach = createReach("native execution retirement", EXECUTION_RETIREMENT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("queued-cancel", "queued-then-size", "immediate-size"),
        fc.boolean(),
        fc.integer({ min: 1, max: 16 }),
        fc.integer({ min: 16, max: 1024 }),
        fc.boolean(),
        async (mode, dispose, rows, bytes, yieldBoundary) => {
          reach.beginHistory();
          const governor = new Governor(8 << 20);
          const blocker =
            mode === "immediate-size"
              ? null
              : await governor.admit({
                  label: "queued launch blocker",
                  minimumBytes: 7 << 20,
                  spillable: false,
                });
          let enter: (() => void) | undefined, release: (() => void) | undefined;
          const entered = new Promise<void>((resolve) => {
            enter = resolve;
          });
          const blocked = new Promise<void>((resolve) => {
            release = resolve;
          });
          let aborted = false;
          const realNow = taskClock.now;
          let tick = 0;
          if (yieldBoundary) {
            taskClock.now = () => tick;
            beginTask();
          }
          const source = { job: null as Promise<unknown> | null };
          const snapshot = {
            catalog: { version: 0 },
            observedLsn: 1,
            pages: {
              peek: () => null,
              read: () => {
                throw new Error("unexpected disk IO");
              },
            },
            table: () => ({
              columns: [{ id: 0 }],
              liveRowCount: rows,
              segments: [
                {
                  kind: "memory",
                  rowCount: rows,
                  liveRowCount: rows,
                  readHint(
                    _request: unknown,
                    context: {
                      budget: { allocate(n: number): boolean; free(n: number): void };
                      signal: AbortSignal;
                    },
                  ) {
                    expect(context.budget.allocate(bytes)).toBe(true);
                    const abort = () => {
                      aborted = true;
                    };
                    context.signal.addEventListener("abort", abort, { once: true });
                    enter?.();
                    const hint = {
                      rowCount: rows,
                      outputBytes: 4 * rows,
                      residentBytes: bytes,
                      scratchBytes: bytes,
                      ioBytes: 0,
                    };
                    if (yieldBoundary) {
                      context.budget.free(bytes);
                      tick = 10000;
                      return hint;
                    }
                    return (async () => {
                      try {
                        await blocked;
                        context.signal.throwIfAborted();
                        return hint;
                      } finally {
                        context.signal.removeEventListener("abort", abort);
                        context.budget.free(bytes);
                      }
                    })();
                  },
                },
              ],
            }),
          } as unknown as Snapshot;
          const plan: PhysicalPlan = {
            root: { id: 1, op: "values", rows: [], output: [column] },
            columns: [column],
            parameters: [],
            placeholderCount: 0,
            clockSlot: null,
            initPlans: [],
            catalogVersion: 0,
            volatile: false,
          };
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
            resultWindowBytes: 64,
          };
          const program: Program = (runtime) => {
            source.job = Promise.resolve(
              boundSource(
                snapshot,
                {
                  id: 1,
                  tableId: 1,
                  columns: [0],
                  types: [INT],
                  windowBytes: 0,
                  stagingBytes: 0,
                  rows: null,
                },
                governor,
                runtime.signal,
                4096,
              ),
            );
            return abortable(
              source.job.then(() => undefined),
              runtime.signal,
            );
          };
          const execution = new QueryExecution(plan, request, program);
          const fence = resourceFence(execution);
          let retired = false;
          void fence.then(
            () => {
              retired = true;
            },
            () => {
              retired = true;
            },
          );
          try {
            if (mode !== "queued-cancel") {
              blocker?.close();
              await entered;
              reach.reach(
                mode === "queued-then-size" ? "launch then program" : "immediate program",
              );
            }
            if (dispose) execution.results.cancel("dispose");
            else execution.cancel("cancel");
            reach.reach(dispose ? "dispose" : "cancel");
            await Promise.allSettled([execution.executed, execution.delivered]);
            reach.reach("delivery remains prompt");
            if (mode === "queued-cancel") {
              await fence;
              expect(source.job).toBeNull();
              expect(governor.usedBytes).toBe(blocker?.held);
              reach.reach("queued admission aborted");
            } else {
              expect(aborted, "cancellation reaches sizing IO immediately").toBe(true);
              reach.reach("IO abort prompt");
              reach.reach(yieldBoundary ? "task-yield cancellation" : "IO cancellation");
              expect(retired, "delivery is not the native resource fence").toBe(false);
              expect(governor.usedBytes).toBeGreaterThan(0);
              release?.();
              await fence;
              expect(governor.usedBytes).toBe(0);
              reach.reach("sizing drained before retirement");
            }
          } finally {
            release?.();
            blocker?.close();
            execution.cancel("cleanup");
            await source.job?.catch(() => undefined);
            await Promise.allSettled([execution.executed, execution.delivered, fence]);
            taskClock.now = realNow;
            beginTask();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero charges");
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          ["queued-cancel", false, 1, 16, false],
          ["queued-cancel", true, 3, 64, true],
          ["queued-then-size", false, 2, 32, false],
          ["queued-then-size", true, 4, 128, true],
          ["immediate-size", false, 5, 256, true],
          ["immediate-size", true, 6, 512, false],
        ],
      },
    );
    reach.assertReached();
  },
);
