/** Inspection funds detached metadata only, releases failures, and retries a changed source. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, INSPECTION_ADMISSION_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { inspectReply } from "../owner/admin-reply.js";

for (const seed of seedsFor("next-inspection-admission", [46966, 46967]))
  it(`waits cancellably without stale metadata or leaked credit (${String(seed)})`, async () => {
    const reach = createReach("inspection admission", INSPECTION_ADMISSION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.integer({ min: 1, max: 8192 }),
        fc.boolean(),
        async (cancel, width, driftVersion) => {
          reach.beginHistory();
          const governor = new Governor(2 << 20),
            abort = new AbortController();
          const grants = await governor.admitGroup({
            label: "competitor",
            includesProgress: true,
            grants: [{ label: "competitor", minimumBytes: governor.capBytes, spillable: false }],
          });
          const competitor = grants[0];
          if (!competitor) throw new Error("No competitor");
          expect(competitor.allocate(governor.capBytes)).toBe(true);
          let version = 1,
            size = 512,
            builds = 0;
          const pending = inspectReply(
            governor,
            () => version,
            () => size,
            () => {
              builds++;
              return { version, width };
            },
            abort.signal,
          );
          // The admission is blocked synchronously by the competitor; changing the source
          // before freeing it deterministically covers version drift, without wall-clock waits.
          if (driftVersion) version = 2;
          size += width;
          if (cancel) {
            const reason = new Error("cancel inspection");
            const checked = expect(pending).rejects.toBe(reason);
            abort.abort(reason);
            await checked;
            reach.reach("cancel queued admission");
            expect(builds).toBe(0);
          } else {
            competitor.close();
            const result = await pending;
            expect(result.value).toEqual({ version, width });
            expect(builds).toBe(1);
            expect(governor.usedBytes).toBe(size);
            result.close();
            result.close();
            expect(() => result.value).toThrow("closed");
            reach.reach(driftVersion ? "retry changed source" : "retry changed recipe");
            reach.reach("idempotent relinquish");
          }
          competitor.close();
          const failure = new Error("build metadata");
          await expect(
            inspectReply(
              governor,
              () => version,
              () => size,
              () => {
                throw failure;
              },
            ),
          ).rejects.toBe(failure);
          expect(governor.usedBytes).toBe(0);
          reach.reach("failed build releases charge");
        },
      ),
      {
        seed,
        numRuns: 14,
        examples: [
          [false, 1, true],
          [true, 8192, false],
          [false, 8192, false],
        ],
      },
    );
    reach.assertReached();
  });
