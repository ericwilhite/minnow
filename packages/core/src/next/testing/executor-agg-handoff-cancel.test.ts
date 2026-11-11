/** Every awaited handoff boundary retains an owner for the sealed spill files. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { ParameterCheckpoint } from "../exec/parameter-checkpoint.js";
import { QueryRuntime } from "../exec/run.js";
import { ResultSink } from "../exec/result-sink.js";
import { RestoredAggregateOperator } from "../exec/agg/restored.js";
import type { Execution } from "../exec/executor.js";
import { Governor } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { ScratchSpillService } from "../spill/scratch-spill.js";
import type { SpillService } from "../spill/contract.js";
import { FileScratch } from "../storage/file-store.js";
import { MemoryFile } from "../storage/memory-store.js";
import { StorageCore } from "./storage-core.js";

const stages = [
  "spool",
  "suspend",
  "sink",
  "resources",
  "retired",
  "admission",
  "parameters",
  "restored",
  "restore",
  "close",
] as const;
type Stage = (typeof stages)[number];
async function deadline<T>(work: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Handoff cancellation stalled")), 30000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

it.each(seedsFor("next-aggregation-handoff-await-cancel", [0xb4e3, 0xb4e4]))(
  "owns spill files across every handoff await until cancellation drains (seed %i)",
  async (seed) => {
    const reach = createReach("aggregate handoff await cancellation", [
      ...stages,
      "synchronous cancel",
      "microtask cancel",
      "memory spill",
      "scratch spill",
      "owned files at cancellation",
      "snapshot retained",
      "zero before shared close",
    ]);
    const core = await new StorageCore(8192).open();
    const scratch = new ScratchSpillService(
      new FileScratch(
        new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null }),
        () => undefined,
      ),
    );
    try {
      await core.execute("CREATE TABLE t(v TEXT)");
      await core.writeBatch(
        "insertBatch",
        "t",
        Array.from({ length: 32768 }, (_, i) => ({ v: `v${String(i)}${"x".repeat(120)}` })),
      );
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      const prepared = core.prepare("SELECT count(DISTINCT v) n,$1::text marker FROM t");
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...stages),
          fc.boolean(),
          fc.integer({ min: 1, max: 8192 }),
          fc.boolean(),
          async (stage, microtask, parameterBytes, onScratch) => {
            reach.beginHistory();
            const spill: SpillService = onScratch ? scratch : core.spill;
            const governor = new Governor(24 << 20);
            const competitor = await governor.admit({
              label: "temporary competing holder",
              minimumBytes: 0,
              spillable: true,
            });
            const snapshot = core.harness.path.pin();
            let idleTarget: Parameters<QueryRuntime["suspendProducer"]>[1] | undefined;
            let execution: Execution | undefined;
            let pressured = false,
              selected = false,
              cancelled = false,
              restoring = false,
              bytesAtCancel = 0;
            const cancelAt = (at: Stage) => {
              if (at !== stage || selected) return;
              selected = true;
              const cancel = () => {
                bytesAtCancel = spill.bytesInUse;
                expect(snapshot.released).toBe(false);
                if (!execution) throw new Error("Cancellation preceded execution creation");
                execution.cancel(new Error(`cancel at ${at}`));
                cancelled = true;
                reach.reach(at);
                reach.reach(microtask ? "microtask cancel" : "synchronous cancel");
                reach.reach("snapshot retained");
              };
              if (microtask) queueMicrotask(cancel);
              else cancel();
            };
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Originals are invoked with .call(this).
            const suspend = QueryRuntime.prototype.suspendProducer;
            const retirement = vi
              .spyOn(QueryRuntime.prototype, "suspendProducer")
              .mockImplementation(async function (this: QueryRuntime, sink, pool) {
                idleTarget = pool;
                cancelAt("suspend");
                await suspend.call(this, sink, pool);
                cancelAt("retired");
              });
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with .call(this).
            const suspendSink = ResultSink.prototype.suspendBudget;
            const sink = vi
              .spyOn(ResultSink.prototype, "suspendBudget")
              .mockImplementation(async function (this: ResultSink) {
                await suspendSink.call(this);
                cancelAt("sink");
              });
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with .call(this).
            const restore = QueryRuntime.prototype.restorePhase;
            const admission = vi
              .spyOn(QueryRuntime.prototype, "restorePhase")
              .mockImplementation(function (this: QueryRuntime, ...args) {
                const work = restore.call(this, ...args);
                if (stage !== "restored") return work;
                return Promise.resolve(work).then(() => cancelAt("restored"));
              });
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with .call(this).
            const restoreParameters = ParameterCheckpoint.prototype.restore;
            const parameters = vi
              .spyOn(ParameterCheckpoint.prototype, "restore")
              .mockImplementation(function (this: ParameterCheckpoint, charge) {
                cancelAt("parameters");
                return restoreParameters.call(this, charge);
              });
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with .call(this).
            const finish = RestoredAggregateOperator.prototype.finish;
            const restored = vi
              .spyOn(RestoredAggregateOperator.prototype, "finish")
              .mockImplementation(function (this: RestoredAggregateOperator) {
                restoring = true;
                cancelAt("restore");
                return finish.call(this);
              });
            const native = governor.admitGroup.bind(governor);
            const transfer = governor.transferCredit.bind(governor);
            governor.transferCredit = (source, target, bytes) => {
              const moved = transfer(source, target, bytes);
              // The next await retires the producer's remaining resource accounts.
              if (moved && target === idleTarget) cancelAt("resources");
              return moved;
            };
            governor.admitGroup = (options) => {
              const work = native(options);
              if (options.idleCredit && options.grants.some((g) => g.minimumBytes > 0)) {
                cancelAt("admission");
                // Completion of the external holder is independent of the waiter.
                competitor.close();
              }
              return work;
            };
            const create = spill.create.bind(spill);
            const files = vi.spyOn(spill, "create").mockImplementation(async (queryId, budget) => {
              const file = await create(queryId, budget);
              const append = file.append.bind(file),
                close = file.close.bind(file);
              file.append = async (page) => {
                const ref = await append(page);
                cancelAt("spool");
                return ref;
              };
              file.close = () => {
                if (restoring) cancelAt("close");
                return close();
              };
              return file;
            });
            try {
              execution = prepared.execute({
                snapshot,
                statement: null,
                parameters: ["p".repeat(parameterBytes)],
                statementTime: 0n,
                governor,
                spill,
                queryId: 1,
                sequences: null,
                sequenceSession: null,
                returning: "atExecuted",
                reads: {
                  open: (pages, chunk, options) => {
                    if (!pressured) {
                      pressured = true;
                      const room = governor.capBytes - governor.reserveBytes - governor.usedBytes;
                      expect(competitor.allocate(Math.max(0, room))).toBe(true);
                    }
                    return ChunkReader.open(pages, chunk, options);
                  },
                },
              });
              const active = execution;
              const settled = Promise.allSettled([active.executed, active.delivered]);
              const consume = async () => {
                for (;;) {
                  const frame = await active.results.next();
                  if (!frame) return;
                  frame.release();
                }
              };
              const [, results] = await deadline(
                Promise.all([consume().catch(() => undefined), settled]),
              );
              expect(cancelled).toBe(true);
              expect(pressured).toBe(true);
              expect(bytesAtCancel).toBeGreaterThan(0);
              for (const result of results)
                expect(result).toMatchObject({
                  status: "rejected",
                  reason: { code: "MINNOW_QUERY_CANCELLED", sqlState: "57014" },
                });
              reach.reach("owned files at cancellation");
            } finally {
              execution?.cancel();
              competitor.close();
              await Promise.allSettled(execution ? [execution.executed, execution.delivered] : []);
              snapshot.release();
              for (const mock of [retirement, sink, admission, parameters, restored, files])
                mock.mockRestore();
            }
            // Successful histories never close or sweep either shared service.
            try {
              expect(governor.usedBytes).toBe(0);
              expect(spill.bytesInUse).toBe(0);
              reach.reach(onScratch ? "scratch spill" : "memory spill");
              reach.reach("zero before shared close");
            } finally {
              // A failing parent/mutant must not contaminate fast-check's next
              // history or shrink. This runs after the leak assertion, never before.
              if (spill.bytesInUse !== 0) await spill.sweep();
            }
          },
        ),
        {
          seed,
          numRuns: 24,
          examples: stages.flatMap<[Stage, boolean, number, boolean]>((stage, index) => [
            [stage, false, 64, index % 2 === 0],
            [stage, true, 4096, index % 2 !== 0],
          ]),
        },
      );
      reach.assertReached();
    } finally {
      await scratch.close();
      await core.close();
    }
  },
  360000,
);
