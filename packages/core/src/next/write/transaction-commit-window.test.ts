/**
 * A writing transaction keeps its commit window from its first write until it ends
 * (credits.ts `holdCommitWindow`; GOVERNOR-WAIT-POLICY: no hold-and-wait, so the commit never
 * waits while the transaction's rows are counted). Law: every growth of the write owner resolves
 * only once that window is funded, whichever exit funds it (an idle account's transfer, an
 * immediate grow, or a group admission). Repeating the same request is then immediate: the engine
 * re-prepares in a loop until a request is satisfied synchronously (engine.ts `#streamCommit`), so
 * a growth that resolves without the window makes that loop spin without end.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRANSACTION_COMMIT_WINDOW_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { WriteCredits } from "./credits.js";

const MiB = 1 << 20;

for (const seed of seedsFor("next-transaction-commit-window", [53021, 53022]))
  it(`funds the transaction's commit window before any growth resolves (seed ${String(seed)})`, async () => {
    const reach = createReach("transaction commit window", TRANSACTION_COMMIT_WINDOW_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("metadata step" as const, "row growth" as const, "native minimum" as const),
        fc.constantFrom("none" as const, "step only" as const, "step and window" as const),
        fc.integer({ min: 1, max: 4 }),
        async (entry, idle, stepMiB) => {
          reach.beginHistory();
          const governor = new Governor(32 * MiB);
          const minimum = { rows: MiB, metadata: MiB, commits: MiB, merge: MiB };
          const credits = await WriteCredits.open(governor, minimum);
          try {
            // Between transactions idle forecasts go back, the commit window with them.
            credits.reclaimIdleForecasts();
            for (const account of [credits.rows, credits.metadata, credits.commits, credits.merge])
              expect(account.held, "nothing idle is left to transfer").toBe(0);
            // Idle credit in another account that a transfer can move: none, enough for the
            // step alone (it fills the grown account first), or enough for the window too.
            const step = stepMiB * MiB;
            const source = entry === "row growth" ? credits.metadata : credits.rows;
            const target = entry === "row growth" ? credits.rows : credits.metadata;
            const idleBytes = idle === "none" ? 0 : idle === "step only" ? step : step + MiB;
            if (idleBytes > 0) expect(source.tryGrow(idleBytes)).toBe(true);
            credits.holdCommitWindow(true);
            expect(credits.transactionCommitBytes).toBe(minimum.commits);
            expect(credits.commits.held).toBeLessThan(credits.transactionCommitBytes);
            reach.reach("window unfunded at first write");

            const before = governor.usedBytes;
            if (entry === "metadata step") {
              await credits.prepareMetadata(step);
            } else if (entry === "row growth") {
              await credits.growTo(credits.rows, step);
            } else {
              await credits.prepareNativeMinimum(step);
            }
            reach.reach(entry);
            expect(
              credits.commits.held,
              "the transaction's commit window is funded when growth resolves",
            ).toBeGreaterThanOrEqual(credits.transactionCommitBytes);
            expect(target.held, "the step itself is funded").toBeGreaterThanOrEqual(step);
            if (idle === "step and window") {
              expect(governor.usedBytes, "idle credit funds it without admission").toBe(before);
              reach.reach("funded by idle transfer");
            } else
              reach.reach(
                idle === "none" ? "funded by fresh admission" : "idle covers the step only",
              );

            // The engine's re-prepare loop terminates: the same request is now immediate.
            if (entry === "metadata step") expect(credits.prepareMetadata(step)).toBeUndefined();
            else if (entry === "native minimum")
              expect(credits.prepareNativeMinimum(step)).toBeUndefined();
            reach.reach("repeat request is immediate");

            // The transaction ends: the idle window goes back.
            credits.holdCommitWindow(false);
            expect(credits.commits.held).toBe(credits.commits.allocated);
            reach.reach("window returned at transaction end");
          } finally {
            credits.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      { seed, numRuns: 32 },
    );
    reach.assertReached();
  });
