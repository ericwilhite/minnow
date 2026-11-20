/** A slow first owner cannot leave its waiting peer's timeout rejection unobserved. */
import { yieldTask } from "./merge.js";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DIRECTORY_OPENER_SETTLEMENT_REACH } from "../../testing/reach.js";
import { settleDirectoryOpen } from "./directory-publication-compat.test-support.js";
for (const seed of seedsFor("next-directory-opener-settlement", [46982, 46983]))
  it(`retains early peer settlement until its consumer awaits (${String(seed)})`, async () => {
    const reach = createReach("directory opener settlement", DIRECTORY_OPENER_SETTLEMENT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 1, max: 3 }),
        fc.string(),
        async (fail, early, turns, text) => {
          reach.beginHistory();
          let resolveOpen: (value: { text: string }) => void = () => undefined,
            rejectOpen: (reason: unknown) => void = () => undefined;
          const opening = new Promise<{ text: string }>((resolve, reject) => {
              resolveOpen = resolve;
              rejectOpen = reject;
            }),
            error = new Error(text),
            db = { text };
          // The fixture requires a non-rejecting settlement immediately. Observe the returned
          // promise too so a missing-handler mutant fails an assertion, not a runner-level error.
          const waiting = settleDirectoryOpen(opening);
          const observed = waiting.then(
            (value) => ({ status: "fulfilled", value }),
            (error: unknown) => ({ status: "rejected", error }),
          );
          const complete = () => {
            if (fail) rejectOpen(error);
            else resolveOpen(db);
          };
          if (early) complete();
          // Model owner work and its finally/close. No wall clock or deadline is changed.
          for (let i = 0; i < turns; i++) await yieldTask();
          if (!early) complete();
          await yieldTask();
          const outcome = await observed;
          expect(outcome.status).toBe("fulfilled");
          if (!("value" in outcome))
            throw new Error("Opener settlement escaped before consumption");
          if (fail) {
            expect(outcome.value).toEqual({ error });
            expect("error" in outcome.value && outcome.value.error).toBe(error);
            reach.reach("original error retained");
          } else expect(outcome.value).toEqual({ db });
          reach.reach(
            fail
              ? early
                ? "failure before owner work"
                : "failure after owner work"
              : early
                ? "success before owner work"
                : "success after owner work",
          );
          reach.reach("deferred consumer");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [true, true, 3, "fence timeout"],
          [true, false, 1, "late timeout"],
          [false, true, 2, "opened"],
          [false, false, 3, "later"],
        ],
      },
    );
    reach.assertReached();
  });
