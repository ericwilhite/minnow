/** R6 and P1: native minimum splitting is synchronous when uncontended and returns every part. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { beginTask, taskClock } from "../task.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

it.each(seedsFor("next-executor-native-minimum", [0xc439, 0xc43a]))(
  "keeps uncontended native admission inside execute and releases all parts (seed %i)",
  async (seed) => {
    const reach = createReach("executor native minimum", [
      "native-split",
      "synchronous",
      "owned-frame",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: -1000, max: 1000 }), async (value) => {
        reach.beginHistory();
        const core = await new StorageCore(8192).open();
        let run: ReturnType<StorageCore["start"]> | null = null;
        try {
          await core.harness.path.quiesce();
          const prepared = core.prepare("SELECT CAST($1 AS INTEGER) AS n");
          const governor = core.harness.governor;
          const baseline = governor.usedBytes;
          const original = governor.transferCredit.bind(governor);
          const transfer = original;
          const now = taskClock.now;
          let transfers = 0;
          governor.transferCredit = (source, target, bytes) => {
            expect(source.allocated, "a live allocation was used as idle credit").toBe(0);
            expect(source.loaned).toBe(0);
            transfers++;
            return transfer(source, target, bytes);
          };
          try {
            // This checks R6's Promise boundary, independently of host descheduling. No
            // production path stamps a clock here; the test supplies its explicit task clock.
            taskClock.now = () => 0;
            beginTask();
            run = core.start(prepared, [value]);
          } finally {
            taskClock.now = now;
            beginTask();
            governor.transferCredit = original;
          }
          expect(transfers, "the executor bypassed native idle-credit splitting").toBeGreaterThan(
            0,
          );
          reach.reach("native-split");
          expect(run.execution.state).toBe("executed");
          expect(run.execution.summary?.profile.synchronous).toBe(true);
          reach.reach("synchronous");
          const frame = run.execution.results.poll();
          expect(frame).not.toBeNull();
          if (frame === null || frame === undefined)
            throw new Error("synchronous execution produced no frame");
          try {
            expect(
              oracleRows({
                columns: prepared.columns,
                batches: [
                  {
                    length: frame.payload.rowCount,
                    selection: null,
                    columns: frame.payload.columns.map((vector, index) => {
                      const type = prepared.columns[index]?.type;
                      if (vector.form !== "flat" || type === undefined)
                        throw new Error("native test expected a flat typed frame");
                      return { ...vector, type };
                    }),
                  },
                ],
              }),
            ).toEqual([{ n: value }]);
            reach.reach("owned-frame");
          } finally {
            frame.release();
          }
          await run.execution.executed;
          await run.execution.delivered;
          run.release();
          run = null;
          expect(governor.usedBytes).toBe(baseline);
          reach.reach("released");
        } finally {
          if (run) {
            run.execution.cancel(new Error("native minimum test cleanup"));
            await Promise.allSettled([run.execution.executed, run.execution.delivered]);
            run.release();
          }
          await core.close();
        }
      }),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  },
);
