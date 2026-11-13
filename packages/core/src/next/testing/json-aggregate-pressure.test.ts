/** Aggregate growth must retire its graph before a cancellable complete admission. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_AGGREGATE_PRESSURE_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
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
it.each(seedsFor("next-json-aggregation-pressure", [0xb708, 0xb709]))(
  "spills/waits or cancels growth without a memory refusal (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON aggregate pressure", JSON_AGGREGATE_PRESSURE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 4096, max: 4608 }),
        fc.boolean(),
        fc.boolean(),
        async (length, cancel, except) => {
          reach.beginHistory();
          // 64 MiB funds the existing retaining-parent recipe (34+ MiB here).
          // The competing native grant occupies ALL remaining credit, including reserve;
          // raising this cap does not remove the observed cancellable admission wait.
          const core = await new StorageCore().open(),
            governor = new Governor(64 << 20);
          let blocker: Grant | undefined;
          const value = '\u0001汉"\\'.repeat(length),
            base = `SELECT json_agg($1::text) j FROM generate_series(1,96)`,
            // Keep the retaining-parent seam in this suite without masking the separately
            // recorded pre-existing EXCEPT compound-loan defect for multi-MiB TEXT keys.
            sql = except
              ? `(SELECT j->>0 AS j FROM (${base})a) EXCEPT SELECT NULL::text j`
              : `SELECT j::text j FROM (${base})a`;
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
              const held = await admit({
                label: "competing retained progress step",
                includesProgress: true,
                grants: [
                  {
                    label: "competing progress",
                    minimumBytes: governor.capBytes - governor.usedBytes,
                    spillable: false,
                  },
                ],
              });
              blocker = held[0];
              if (!blocker) throw new Error("Missing competing native grant");
              expect(blocker.allocate(blocker.held)).toBe(true);
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
              blocker?.close();
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
              if (except) expect(maxCapacity).toBeGreaterThan(4096);
              else expect(maxCapacity).toBe(0);
              expect(
                core.lastProfile?.operators.reduce((n, p) => n + p.spilledBytes, 0),
              ).toBeGreaterThan(0);
            }
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
            reach.reach(except ? "EXCEPT ancestor" : "unary ancestor");
            reach.reach("growth handoff");
          } finally {
            blocker?.close();
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
          [4096, false, false],
          [4096, true, false],
          [4096, false, true],
          [4096, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
