import fc from "fast-check";
import { afterEach, expect, it, vi, type MockInstance } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PROTOTYPE_RUN_LIFETIME_REACH } from "../../testing/reach.js";
import { PrototypeRuns } from "./prototype-runs.test-support.js";

function gate() {
  let open: () => void = () => undefined;
  const promise = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { promise, open: () => open() };
}

class Probe {
  read(): string {
    return "original";
  }
}

afterEach(() => vi.restoreAllMocks());

it.each(seedsFor("next-prototype-run-lifetime", [0xb79b]))(
  "drains cancelled and failed histories before the next prototype observer, seed %i",
  async (seed) => {
    const reach = createReach("prototype observer lifetime", PROTOTYPE_RUN_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.boolean(), fc.boolean(), async (early, failure, task) => {
        reach.beginHistory();
        const runs = new PrototypeRuns();
        const abort = new AbortController();
        const error = new Error(failure ? "query failed" : "test cancelled");
        const installed = gate(),
          cleaning = gate(),
          cleanup = gate();
        let nextSpy: MockInstance<Probe["read"]> | undefined;
        if (early) abort.abort(error);
        const pending = runs.run(async () => {
          const oldSpy = vi.spyOn(Probe.prototype, "read").mockReturnValue("old");
          installed.open();
          try {
            if (failure) throw error;
            abort.signal.throwIfAborted();
            await new Promise<never>((_resolve, reject) => {
              abort.signal.addEventListener("abort", () => reject(error), {
                once: true,
              });
            });
          } finally {
            cleaning.open();
            await cleanup.promise;
            oldSpy.mockRestore();
          }
        });
        const observed = pending.catch((reason: unknown) => {
          expect(reason).toBe(error);
        });
        try {
          await installed.promise;
          if (!early) abort.abort(error);
          await cleaning.promise;
          expect(new Probe().read()).toBe("old");
          const next = runs.settled().then(() => {
            nextSpy = vi.spyOn(Probe.prototype, "read").mockReturnValue("next");
          });
          if (task) await new Promise<void>((resolve) => setTimeout(resolve, 0));
          else await Promise.resolve();
          cleanup.open();
          await Promise.all([next, observed]);
          expect(new Probe().read()).toBe("next");
          reach.reach("next observer intact");
          reach.reach("deferred cleanup");
          reach.reach(task ? "task gap" : "microtask gap");
          reach.reach(
            failure ? "query error" : early ? "already cancelled" : "cancelled in flight",
          );
        } finally {
          cleanup.open();
          await observed;
          await runs.settled();
          nextSpy?.mockRestore();
        }
      }),
      {
        seed,
        numRuns: 16,
        examples: [
          [true, false, false],
          [false, false, true],
          [false, true, false],
        ],
      },
    );
    reach.assertReached();
  },
);
