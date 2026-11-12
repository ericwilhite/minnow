/** Constructor growth must retire its graph before a cancellable complete admission. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_CONSTRUCT_PRESSURE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import type { Execution } from "../exec/executor.js";
import { JsonPhaseOperator } from "../exec/json-phase.js";
import { yieldTask } from "../exec/pipeline.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("next-json-construction-pressure", [0xb6d2, 0xb6d3]))(
  "spills/waits or cancels growth without a memory refusal (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON constructor pressure", JSON_CONSTRUCT_PRESSURE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 60000, max: 80000 }),
        fc.boolean(),
        fc.boolean(),
        async (length, cancel, except) => {
          reach.beginHistory();
          const core = await new StorageCore().open(),
            governor = new Governor(32 << 20);
          const blocker = await governor.admit({
            label: "competing retained value",
            minimumBytes: 0,
            spillable: true,
          });
          const value = '\u0001汉"\\'.repeat(length),
            base = `SELECT json_build_object('a',$1::text,'b',json_build_array(NULL,3))::text j`,
            sql = except ? `(${base}) EXCEPT SELECT NULL::text j` : base;
          const expected = await pg.query(sql, [value]);
          const prepare = core.executor.prepare.bind(core.executor);
          let execution: Execution | undefined,
            waited = false,
            maxCapacity = 0;
          const prep = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
            const p = prepare(plan);
            return {
              ...p,
              execute(request) {
                execution = p.execute({ ...request, governor });
                return execution;
              },
            };
          });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- actual receiver passed below
          const finish = JsonPhaseOperator.prototype.finish;
          const phases = vi
            .spyOn(JsonPhaseOperator.prototype, "finish")
            .mockImplementation(function (this: JsonPhaseOperator) {
              maxCapacity = Math.max(maxCapacity, this.shared.capacity);
              return finish.call(this);
            });
          const admit = governor.admitGroup.bind(governor);
          const groups = vi.spyOn(governor, "admitGroup").mockImplementation(async (options) => {
            const total = options.grants.reduce((n, p) => n + p.minimumBytes, 0);
            const block =
              !waited && options.idleCredit !== undefined && total > options.idleCredit.held;
            if (block) {
              waited = true;
              expect(
                blocker.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
              ).toBe(true);
            }
            const pending = admit(options);
            let settled = false;
            void pending.then(
              () => {
                settled = true;
              },
              () => {
                settled = true;
              },
            );
            if (block) {
              await yieldTask();
              await yieldTask();
              expect(
                settled,
                "growth must wait while another value owns the available credit",
              ).toBe(false);
              reach.reach("observed admission wait");
              if (cancel) {
                execution?.cancel();
                reach.reach("cancel waiting admission");
              } else reach.reach("resume waiting admission");
              blocker.close();
            }
            return pending;
          });
          try {
            if (cancel)
              await expect(core.execute(sql, [value])).rejects.toMatchObject({ sqlState: "57014" });
            else {
              const result = await core.execute(sql, [value]);
              if (!("batches" in result)) throw new Error("Expected rows");
              expect(oracleRows(result)).toEqual(expected.rows);
              reach.reach("exact text");
            }
            expect(waited).toBe(true);
            if (!cancel) {
              expect(maxCapacity).toBeGreaterThan(4096);
              expect(
                core.lastProfile?.operators.reduce((n, p) => n + p.spilledBytes, 0),
              ).toBeGreaterThan(0);
            }
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
            reach.reach(except ? "EXCEPT ancestor" : "unary ancestor");
            reach.reach("growth handoff");
          } finally {
            blocker.close();
            execution?.cancel();
            await Promise.allSettled(execution ? [execution.executed, execution.delivered] : []);
            groups.mockRestore();
            phases.mockRestore();
            prep.mockRestore();
            await core.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero resources");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [60000, false, false],
          [60000, true, false],
          [60000, false, true],
          [60000, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
