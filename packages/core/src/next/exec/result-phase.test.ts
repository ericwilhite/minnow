import { ReservedGrant } from "./admission.js";
import { StepLoans } from "../memory/step-loans.js";
/** Result frames belong to the producer phase until consumer credit, not to whichever
 * native budget happens to be installed when that credit arrives. The phase boundary drains
 * them before transferCredit moves idle credit. Law: ResultSink.suspendBudget contract.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import type { Batch } from "../vector/vector.js";
import { DeferredGrant } from "./deferred-grant.js";
import { dictionary, texts, TEXT } from "./kernels/test-support.js";
import { ResultChannel } from "./result.js";
import { ResultSink } from "./result-sink.js";

it.each(seedsFor("result-phase-credit", [0xb4bd, 0xb4be]))(
  "waits for frames, transfers idle credit without a gap and cancels (seed %i)",
  async (seed) => {
    const reach = createReach("result phase credit", [
      "outstanding frame",
      "early detach rejected",
      "idle transfer",
      "dictionary IDs survive",
      "cancel frame wait",
      "zero charges",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 3, max: 8 }), async (rows) => {
        reach.beginHistory();
        for (const cancel of [false, true]) {
          const governor = new Governor(8 << 20),
            old = await governor.admit({
              label: "old result",
              minimumBytes: 1 << 20,
              spillable: false,
            }),
            [next] = await governor.admitGroup({
              label: "next result",
              grants: [{ label: "next result", minimumBytes: 0, spillable: false }],
            });
          if (!next) throw new Error("Missing next native result grant");
          const budget = new DeferredGrant("result binding");
          budget.install(old, () => undefined);
          const channel = new ResultChannel([{ name: "v", type: TEXT, nullable: false }], {
              windowBytes: 4096,
            }),
            sink = new ResultSink([TEXT], budget, channel, 17),
            source = texts(["one", "two", "three"]),
            batch: Batch = {
              length: rows,
              selection: null,
              columns: [
                dictionary(
                  source,
                  Array.from({ length: rows }, (_, i) => i % 3),
                ),
              ],
            };
          try {
            expect(sink.push(batch)).toBe("ready");
            const frame = channel.poll();
            if (!frame) throw new Error("Missing phase frame");
            const firstId = frame.payload.dictionaries[0]?.id;
            if (firstId === undefined) throw new Error("Dictionary witness missing");
            let settled = false;
            const pause = sink.suspendBudget();
            void pause.then(
              () => {
                settled = true;
              },
              () => {
                settled = true;
              },
            );
            await Promise.resolve();
            await Promise.resolve();
            expect(settled).toBe(false);
            expect(() => {
              const unexpected = budget.detach();
              budget.install(unexpected, () => undefined);
            }).toThrow("Result frames must return");
            reach.reach("outstanding frame");
            reach.reach("early detach rejected");
            if (cancel) {
              const failure = new Error("cancel phase frame wait");
              sink.discard(failure);
              channel.cancel(failure);
              await expect(pause).rejects.toBe(failure);
              frame.release(); // Already discarded: cannot charge/free the next epoch.
              reach.reach("cancel frame wait");
            } else {
              frame.release();
              await pause;
              expect(old.allocated).toBe(0);
              const detached = budget.detach(),
                used = governor.usedBytes;
              expect(governor.transferCredit(detached, next, detached.held)).toBe(true);
              expect(governor.usedBytes).toBe(used);
              detached.close();
              budget.install(next, () => undefined);
              sink.resumeBudget();
              expect(sink.push(batch)).toBe("ready");
              const second = channel.poll();
              if (!second) throw new Error("Missing resumed frame");
              expect(second.payload.dictionaries[0]?.id).toBeGreaterThan(firstId);
              second.release();
              await sink.finish();
              await channel.delivered;
              expect(sink.rowCount).toBe(rows * 2);
              reach.reach("idle transfer");
              reach.reach("dictionary IDs survive");
            }
          } finally {
            sink.discard(new Error("cleanup"));
            budget.close();
            expect(old.allocated + next.allocated).toBe(0);
            old.close();
            next.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero charges");
        }
      }),
      { seed, numRuns: 6 },
    );
    reach.assertReached();
  },
  30000,
);

it("parks production fixed/growth/progress native parts without stale wrapper credit", async () => {
  const governor = new Governor(8 << 20);
  const parts = await governor.admitGroup({
    label: "production result",
    includesProgress: true,
    grants: [
      { label: "fixed", minimumBytes: 1 << 20, spillable: false },
      { label: "growth", minimumBytes: 4096, spillable: true },
      { label: "progress", minimumBytes: 1 << 20, spillable: false },
      { label: "pool", minimumBytes: 0, spillable: false },
    ],
  });
  const [fixed, growth, lane, pool] = parts;
  if (!fixed || !growth || !lane || !pool) throw new Error("parts missing");
  const reserved = new ReservedGrant(
    fixed,
    growth,
    1 << 20,
    new StepLoans(lane, 1 << 20, "result step"),
    false,
    () => lane.close(),
    true,
  );
  const binding = new DeferredGrant("production result");
  binding.install(reserved, () => undefined);
  await governor.admitGroup({ label: "register idle", idleCredit: pool, grants: [] });
  const held = governor.usedBytes;
  const old = binding.detach();
  expect(old).toBe(reserved);
  reserved.parkIdle(governor, pool, lane);
  expect(reserved.held).toBe(0);
  expect(pool.held).toBe(held);
  expect(governor.usedBytes).toBe(held);
  binding.close();
  expect(pool.held).toBe(held);
  pool.close();
  expect(governor.usedBytes).toBe(0);
});
