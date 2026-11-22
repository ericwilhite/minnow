/**
 * A commit funding its window at commit time, under memory pressure. A native step keeps the
 * commit window (`nativeCommitBytes`), but idle reclaim between statements returns it, so the
 * commit of a transaction that went idle funds its window when it starts (`prepareCommit`), as
 * holder growth while the transaction's rows stay protected. Checked at the credits and
 * governor level. Laws (GOVERNOR-WAIT-POLICY):
 * - the commit funding is served before an earlier, queued new admission whenever it fits beside
 *   the step (holder priority), and is refused with 53200 at once when it cannot;
 * - nothing stalls (the stall guard is on);
 * - once the commit's window is retired the earlier admission is served if it fits;
 * - the cap is never exceeded.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_COMMIT_FUNDING_REACH } from "../../testing/reach.js";
import { Governor, MemoryAdmissionError, type GrantEvent } from "../memory/governor.js";
import { WriteCredits, type PreparedWriteCredit } from "./credits.js";

const KiB = 1024,
  MiB = 1024 * KiB;

for (const seed of seedsFor("next-native-commit-funding", [51031, 51032]))
  it(`funds a commit window at commit time under pressure (${String(seed)})`, async () => {
    const reach = createReach("native commit funding", NATIVE_COMMIT_FUNDING_REACH);
    const previousStall = Governor.stallAfterMs;
    Governor.stallAfterMs = previousStall ?? 2_000;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(8, 16, 32),
          fc.integer({ min: 1, max: 8 }),
          fc.integer({ min: 64, max: 2_048 }),
          fc.integer({ min: 1, max: 100 }),
          fc.boolean(),
          async (capMib, commitMib, stepKiB, foreignPercent, deplete) => {
            reach.beginHistory();
            const governor = new Governor(capMib * MiB);
            const ordinary = governor.capBytes - governor.reserveBytes;
            const minimum = {
              rows: 256 * KiB,
              metadata: 64 * KiB,
              commits: commitMib * MiB,
              merge: 256 * KiB,
            };
            if (minimum.rows + minimum.metadata + minimum.commits + minimum.merge > ordinary)
              return;
            const credits = await WriteCredits.open(governor, minimum);
            const grants: GrantEvent[] = [];
            const unobserve = governor.observeGrants((event) => grants.push(event));
            let foreign: Promise<unknown> | null = null;
            let primary: PreparedWriteCredit | null = null;
            const foreignClosed = { done: false };
            try {
              if (deplete) {
                credits.reclaimIdleForecasts();
                reach.reach("forecasts retired before the step");
              }
              // The transaction's rows, protected; then it goes idle between statements, and idle
              // reclaim returns its forecasts, the commit window included.
              await credits.prepareMetadata(16 * KiB);
              const step = stepKiB * KiB;
              await credits.growTo(credits.rows, credits.rows.allocated + step + 4 * KiB);
              primary = credits.protectRows(step);
              credits.reclaimIdleForecasts();
              expect(credits.commits.held, "idle reclaim returned the commit window").toBe(
                credits.commits.allocated,
              );
              // Pressure: an earlier new admission for more than is free now.
              const free = ordinary - governor.usedBytes;
              const foreignBytes = Math.max(
                1,
                Math.min(ordinary, Math.floor((free * (100 + foreignPercent)) / 100)),
              );
              foreign = governor
                .admit({ label: "earlier foreign", minimumBytes: foreignBytes, spillable: false })
                .then(
                  (grant) => {
                    grant.close();
                    foreignClosed.done = true;
                    return "served";
                  },
                  (error: unknown) => error,
                );
              await Promise.resolve();
              // The commit window, funded when the commit starts.
              let outcome: unknown = "served";
              try {
                await credits.prepareCommit();
              } catch (error) {
                outcome = error;
              }
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              if (outcome === "served") {
                expect(credits.commits.held).toBeGreaterThanOrEqual(minimum.commits);
                // Holder priority, at the grant: no new admission was granted past the queued
                // commit growth.
                for (const event of grants)
                  if (event.source === "admission")
                    expect(event.queued.holders, `${event.label} past queued holder growth`).toBe(
                      0,
                    );
                if (grants.some((event) => event.source === "holder"))
                  reach.reach("commit funded as holder growth");
                if (!foreignClosed.done) reach.reach("served ahead of an earlier admission");
              } else {
                // Refused only above the cap (the governor law), never below it.
                expect(outcome).toBeInstanceOf(MemoryAdmissionError);
                expect(outcome).toMatchObject({ sqlState: "53200" });
                const refused = outcome as MemoryAdmissionError & {
                  bytes: number;
                  capacity: number;
                };
                expect(refused.bytes).toBeGreaterThan(refused.capacity);
                reach.reach("refused above the cap");
              }
              // A served commit runs and retires its window (a refused one never ran); the step ends.
              if (outcome === "served") await credits.prepareCommit();
              primary.release();
              primary = null;
              credits.reclaimIdleForecasts();
              const room = ordinary - governor.usedBytes;
              const settled = await Promise.race([
                foreign,
                new Promise((resolve) => setTimeout(() => resolve("pending"), 4_000)),
              ]);
              if (foreignClosed.done || foreignBytes <= room) {
                expect(settled, "the earlier admission after the commit's window retired").not.toBe(
                  "pending",
                );
                if (settled === "served") reach.reach("earlier admission served afterwards");
              }
            } finally {
              unobserve();
              primary?.release();
              credits.close();
              await foreign;
            }
            expect(governor.usedBytes).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 40,
          examples: [
            [8, 6, 256, 50, true],
            [32, 6, 2_048, 1, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      Governor.stallAfterMs = previousStall;
    }
  }, 120_000);
