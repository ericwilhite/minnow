/** Simulate Vitest abandoning an async history; never wait for a real 120 s timeout. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, COMPOUND_HISTORY_REACH } from "../../testing/reach.js";
import { QueryRuntime } from "../exec/run.js";
import {
  PressureCampaign,
  admitGroup,
  reserveWorkingSet,
  retirePressureHistories,
} from "./compound-progress-history.test.fixture.js";

it.each(seedsFor("next-compound-progress-abandoned-histories", [0xb726, 0xb727]))(
  "retires abandoned observers and executions before the next seed (seed %i)",
  async (seed) => {
    const reach = createReach<string>("compound test owners", COMPOUND_HISTORY_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.integer({ min: 1, max: 4 }), async (queued, repeats) => {
        reach.beginHistory();
        const campaign = new PressureCampaign(),
          history = await campaign.history(16 << 20),
          { core, governor } = history;
        const lateReserveClose = history.observe(() =>
          vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(reserveWorkingSet),
        );
        const lateAdmissionClose = history.observe(() =>
          vi.spyOn(governor, "admitGroup").mockImplementation(function (options) {
            return admitGroup.call(governor, options);
          }),
        );
        const occupied = queued
          ? await governor.admit({
              label: "other-query",
              minimumBytes: governor.capBytes - governor.reserveBytes,
              spillable: false,
            })
          : null;
        try {
          if (queued) {
            const run = core.start(history.wrap(core.prepare("SELECT 1")));
            history.ownCleanup(run.release);
            expect(run.execution.state).not.toBe("executed");
          }
          // This is the same cleanup hook Vitest runs when the body has been abandoned.
          await retirePressureHistories();
          // eslint-disable-next-line @typescript-eslint/unbound-method -- identity assertion.
          expect(QueryRuntime.prototype.reserve).toBe(reserveWorkingSet);
          // eslint-disable-next-line @typescript-eslint/unbound-method -- identity assertion.
          expect(governor.admitGroup).toBe(admitGroup);
          reach.reach("afterEach restoration");
          await expect(campaign.history(16 << 20)).rejects.toThrow("Compound progress test ended");
          expect(() => history.guard()).toThrow("Compound progress test ended");
          let installed = false;
          expect(() =>
            history.observe(() => {
              installed = true;
              return { mockRestore: () => undefined };
            }),
          ).toThrow("Compound progress test ended");
          expect(installed).toBe(false);
          reach.reach("abandoned observer stopped");
          reach.reach("abandoned shrinker stopped");
          if (queued) reach.reach("queued execution cancelled");
          const nextCampaign = new PressureCampaign(),
            next = await nextCampaign.history(16 << 20);
          try {
            const nextSpy = vi
              .spyOn(QueryRuntime.prototype, "reserve")
              .mockImplementation(reserveWorkingSet);
            next.observe(() => nextSpy);
            for (let i = 0; i < repeats; i++) {
              lateReserveClose();
              lateAdmissionClose();
              await history.close();
            }
            // A previous seed's late finally must not uninstall the new seed's observer.
            // eslint-disable-next-line @typescript-eslint/unbound-method -- identity assertion.
            expect(QueryRuntime.prototype.reserve).toBe(nextSpy);
            reach.reach("late closer inert");
            reach.reach("idempotent close");
          } finally {
            await nextCampaign.close();
          }
        } finally {
          occupied?.close();
          await campaign.close();
        }
        expect(governor.usedBytes).toBe(0);
        expect(core.spill.bytesInUse).toBe(0);
        reach.reach("zero resources");
      }),
      {
        seed,
        numRuns: 6,
        examples: [
          [false, 1],
          [true, 2],
        ],
      },
    );
    reach.assertReached();
  },
);
