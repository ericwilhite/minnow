/** R6: an already-cached bounded scalar does not acquire a gratuitous Promise boundary. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { beginTask, taskClock } from "../task.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
it.each(seedsFor("next-aggregation-synchronous-minimum", [0xb4a7, 0xb4a8]))(
  "keeps uncontended fixed-state aggregation synchronous (seed %i)",
  async (seed) => {
    const core = await new StorageCore().open(),
      reach = createReach("scalar aggregate R6", [
        "executed synchronously",
        "owned result",
        "released",
      ]);
    try {
      const prepared = core.prepare("SELECT SUM($1::int4 + $2::int4) AS n");
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: -1000, max: 1000 }),
          fc.integer({ min: -1000, max: 1000 }),
          async (a, b) => {
            reach.beginHistory();
            const before = core.harness.governor.usedBytes;
            const realClock = taskClock.now;
            const run = (() => {
              try {
                // Test Promise boundaries independently of host descheduling, like the
                // existing native-minimum R6 campaign. Production adds no clock starts.
                taskClock.now = () => 0;
                beginTask();
                return core.start(prepared, [a, b]);
              } finally {
                taskClock.now = realClock;
                beginTask();
              }
            })();
            try {
              expect(run.execution.state).toBe("executed");
              expect(run.execution.summary?.profile.synchronous).toBe(true);
              reach.reach("executed synchronously");
              const frame = run.execution.results.poll();
              if (!frame) throw new Error("Synchronous aggregate did not offer its result");
              try {
                const row = oracleRows({
                  columns: prepared.columns,
                  batches: [
                    {
                      length: frame.payload.rowCount,
                      selection: null,
                      columns: frame.payload.columns.map((vector, i) => {
                        const type = prepared.columns[i]?.type;
                        if (vector.form !== "flat" || !type)
                          throw new Error("Expected owned typed result");
                        return { ...vector, type };
                      }),
                    },
                  ],
                })[0];
                expect(Number(row?.n)).toBe(a + b);
                reach.reach("owned result");
              } finally {
                frame.release();
              }
              await run.execution.executed;
              await run.execution.delivered;
            } finally {
              run.execution.cancel(new Error("sync aggregate cleanup"));
              await Promise.allSettled([run.execution.executed, run.execution.delivered]);
              run.release();
            }
            expect(core.harness.governor.usedBytes).toBe(before);
            reach.reach("released");
          },
        ),
        { seed, numRuns: 10 },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
  },
);
