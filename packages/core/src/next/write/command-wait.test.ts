/** Cancelled commands leave no listener/reaction list on independently stalled maintenance. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, COMMAND_MAINTENANCE_WAIT_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { CommandMaintenance, waitCommandMaintenance } from "./command-wait.js";
it.each(seedsFor("next-command-maintenance-wait", [0xb5f2]))(
  "keeps one completion observer across cancelled commands seed %i",
  async (seed) => {
    const reach = createReach("native command maintenance wait", COMMAND_MAINTENANCE_WAIT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 64 }), fc.boolean(), async (count, fault) => {
        reach.beginHistory();
        let resolve!: () => void, reject!: (reason: unknown) => void;
        const work = new Promise<void>((yes, no) => {
          resolve = yes;
          reject = no;
        });
        const observer = vi.spyOn(work, "then"),
          maintenance = new CommandMaintenance(),
          started = { count: 0 },
          start = () => {
            started.count++;
            return work;
          };
        try {
          for (let row = 0; row < count; row++) {
            const stop = new AbortController(),
              reason = new Error(`cancel command ${String(row)}`),
              remove = vi.spyOn(stop.signal, "removeEventListener");
            const state = { settled: false };
            const pending = maintenance.wait(start, stop.signal).then(
              () => null,
              (error: unknown) => {
                state.settled = true;
                return error;
              },
            );
            stop.abort(reason);
            for (let turn = 0; turn < 4; turn++) await Promise.resolve();
            const cancelledBeforeCompletion = state.settled;
            if (!cancelledBeforeCompletion) resolve(); // Fence a broken waiter before asserting.
            const failure = await pending;
            expect(cancelledBeforeCompletion, "cancel waited for native maintenance").toBe(true);
            expect(failure).toBe(reason);
            expect(remove).toHaveBeenCalledWith("abort", expect.any(Function));
          }
          expect(observer).toHaveBeenCalledTimes(2);
          expect(started.count).toBe(1);
          reach.reach("one completion observer");
          reach.reach("one native operation factory");
          reach.reach("cancelled listeners removed");
          reach.reach("later command attaches");
          const stop = new AbortController(),
            remove = vi.spyOn(stop.signal, "removeEventListener"),
            last = waitCommandMaintenance(work, stop.signal),
            reason = new Error("native maintenance failed");
          await expect(waitCommandMaintenance(work, new AbortController().signal)).rejects.toThrow(
            "Concurrent native maintenance",
          );
          reach.reach("one active waiter");
          if (fault) {
            reject(reason);
            await expect(last).rejects.toBe(reason);
            await expect(waitCommandMaintenance(work, stop.signal)).rejects.toBe(reason);
            reach.reach("owner failure preserved");
          } else {
            resolve();
            await last;
            await waitCommandMaintenance(work, stop.signal);
            reach.reach("owner completion preserved");
          }
          expect(remove).toHaveBeenCalledWith("abort", expect.any(Function));
          expect(observer).toHaveBeenCalledTimes(2);
          reach.reach("settled listener removed");
        } finally {
          resolve();
          observer.mockRestore();
        }
      }),
      {
        seed,
        numRuns: 12,
        examples: [
          [2, false],
          [64, true],
        ],
      },
    );
    reach.assertReached();
  },
);
