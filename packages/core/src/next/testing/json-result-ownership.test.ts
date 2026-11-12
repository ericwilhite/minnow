/** Result-owned bytes stay charged through every borrowed read, fault and cancellation boundary. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_RESULT_OWNERSHIP_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { MemorySpillFile } from "../spill/memory-spill.js";
import { ResultBuffers } from "../exec/result-buffer.js";
import { yieldTask } from "../exec/pipeline.js";
import { StorageCore } from "./storage-core.js";
function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
it.each(seedsFor("next-json-result-ownership", [0xb6dc, 0xb6dd]))(
  "fences partial result reads before grant retirement (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON partial result ownership", JSON_RESULT_OWNERSHIP_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 3 }),
        fc.boolean(),
        fc.boolean(),
        async (cut, cancel, two) => {
          reach.beginHistory();
          const core = await new StorageCore().open(),
            governor = new Governor(16 << 20),
            prepared = core.prepare(
              `SELECT json_build_object('v',$1::text)::text j${two ? ",json_build_array($1::text)::text k" : ""}`,
            ),
            started = gate(),
            releaseIO = gate();
          let owner: Grant | undefined,
            blocked = false,
            hasBacking = false,
            readsAfterAllocation = 0;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- actual receiver supplied below
          const allocate = ResultBuffers.prototype.bytes;
          const allocations = vi
            .spyOn(ResultBuffers.prototype, "bytes")
            .mockImplementation(function (this: ResultBuffers, n) {
              const b = allocate.call(this, n);
              if (n > 65536) {
                hasBacking = true;
                owner = this.budget;
              }
              return b;
            });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- actual receiver supplied below
          const read = MemorySpillFile.prototype.read,
            reason = new Error("injected result-owned read fault");
          const reads = vi
            .spyOn(MemorySpillFile.prototype, "read")
            .mockImplementation(async function (this: MemorySpillFile, ref) {
              const bytes = await read.call(this, ref);
              if (hasBacking && !blocked && ++readsAfterAllocation === cut) {
                blocked = true;
                started.resolve();
                await releaseIO.promise;
                if (!cancel) throw reason;
              }
              return bytes;
            });
          const p = {
              ...prepared,
              execute: (request: Parameters<typeof prepared.execute>[0]) =>
                prepared.execute({ ...request, governor }),
            },
            run = core.start(p, ["\u0001".repeat(200000)]);
          const next = run.execution.results.next();
          void next.catch(() => undefined);
          let delivered = false;
          void run.execution.delivered.then(
            () => {
              delivered = true;
            },
            () => {
              delivered = true;
            },
          );
          try {
            await Promise.race([
              started.promise,
              run.execution.executed.then(() => {
                throw new Error("Reader never reached partial allocation");
              }),
            ]);
            expect(owner).toBeDefined();
            expect(owner?.allocated).toBeGreaterThan(1000000);
            if (cancel) run.execution.cancel();
            await yieldTask();
            await yieldTask();
            expect(delivered, "result ownership cannot retire ahead of its outstanding read").toBe(
              false,
            );
            expect(owner?.allocated).toBeGreaterThan(1000000);
            reach.reach("live result charge");
            reach.reach("delivery fence");
            reach.reach(two ? "two columns" : "one column");
            if (cut > 1) reach.reach("generated read cut");
            releaseIO.resolve();
            if (cancel) {
              await expect(next).rejects.toMatchObject({ sqlState: "57014" });
              await expect(run.execution.executed).rejects.toMatchObject({ sqlState: "57014" });
              await expect(run.execution.delivered).rejects.toMatchObject({ sqlState: "57014" });
              reach.reach("cancel while read");
            } else {
              await expect(next).rejects.toThrow(reason);
              await expect(run.execution.executed).rejects.toThrow(reason);
              await expect(run.execution.delivered).rejects.toThrow(reason);
              reach.reach("read fault");
            }
            expect(core.spill.bytesInUse).toBe(0);
            expect(governor.usedBytes).toBe(0);
            reach.reach("zero resources");
          } finally {
            releaseIO.resolve();
            run.execution.cancel();
            await Promise.allSettled([next, run.execution.executed, run.execution.delivered]);
            run.release();
            reads.mockRestore();
            allocations.mockRestore();
            await core.close();
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, true, false],
          [3, true, true],
          [1, false, true],
          [3, false, false],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
