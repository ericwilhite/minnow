/** Simulate Vitest abandoning an async history; never wait for a real 120 s timeout. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_CONSTRUCT_HISTORY_REACH } from "../../testing/reach.js";
import { ResultBuffers } from "../exec/result-buffer.js";
import {
  JsonCampaign,
  admitGroup,
  allocateResultBytes,
  retireJsonHistories,
} from "./json-construct-history.test.fixture.js";

it.each(seedsFor("next-json-construction-abandoned-histories", [0xb6dc, 0xb6dd]))(
  "retires abandoned observers and executions before the next seed (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON test owners", JSON_CONSTRUCT_HISTORY_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.integer({ min: 1, max: 4 }), async (queued, repeats) => {
        reach.beginHistory();
        const campaign = new JsonCampaign(),
          history = await campaign.history(16 << 20),
          { core, governor } = history;
        const lateResultClose = history.observe(
          vi.spyOn(ResultBuffers.prototype, "bytes").mockImplementation(allocateResultBytes),
        );
        const lateAdmissionClose = history.observe(
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
            history.ownRelease(run.release);
            expect(run.execution.state).not.toBe("executed");
          }
          // This is the same cleanup hook Vitest runs when the body has been abandoned.
          await retireJsonHistories();
          // eslint-disable-next-line @typescript-eslint/unbound-method -- identity assertion.
          expect(ResultBuffers.prototype.bytes).toBe(allocateResultBytes);
          // eslint-disable-next-line @typescript-eslint/unbound-method -- identity assertion.
          expect(governor.admitGroup).toBe(admitGroup);
          reach.reach("afterEach restoration");
          await expect(campaign.history(16 << 20)).rejects.toThrow("JSON construction test ended");
          expect(() => history.guard()).toThrow("JSON construction test ended");
          reach.reach("abandoned shrinker stopped");
          if (queued) reach.reach("queued execution cancelled");
          const nextCampaign = new JsonCampaign(),
            next = await nextCampaign.history(16 << 20);
          try {
            const nextSpy = vi
              .spyOn(ResultBuffers.prototype, "bytes")
              .mockImplementation(allocateResultBytes);
            next.observe(nextSpy);
            for (let i = 0; i < repeats; i++) {
              lateResultClose();
              lateAdmissionClose();
              await history.close();
            }
            // A previous seed's late finally must not uninstall the new seed's observer.
            // eslint-disable-next-line @typescript-eslint/unbound-method -- identity assertion.
            expect(ResultBuffers.prototype.bytes).toBe(nextSpy);
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
