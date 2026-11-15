/** A native row prepayment survives root image reads without a producer loan or hidden wait. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, PREPARED_ROW_CREDIT_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { yieldTask } from "../task.js";
import { WriteCredits, WRITE_ROW_STEP_BYTES } from "./credits.js";
const KiB = 1024;
const minimum = { rows: 64 * KiB, metadata: 64 * KiB, commits: 64 * KiB, merge: 64 * KiB };
it.each(seedsFor("next-prepared-row-credit", [0xb5eb]))(
  "protects prepared writer progress from reclaim/transfers seed %i",
  async (seed) => {
    const reach = createReach("prepared native row progress", PREPARED_ROW_CREDIT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 128 * KiB, max: 512 * KiB }),
        fc.integer({ min: 1, max: 32 * KiB }),
        fc.boolean(),
        async (bytes, live, cancel) => {
          reach.beginHistory();
          const governor = new Governor(4 * 1024 * KiB),
            credits = await WriteCredits.open(governor, minimum);
          let step: ReturnType<WriteCredits["protectRows"]> | null = null,
            holder: Awaited<ReturnType<Governor["admit"]>> | null = null;
          try {
            expect(credits.rows.tryGrow(1024 * KiB)).toBe(true);
            expect(credits.rows.allocate(live)).toBe(true);
            const before = credits.rows.allocated;
            step = credits.protectRows(bytes);
            expect(credits.rows.allocated - before).toBe(WRITE_ROW_STEP_BYTES);
            step.check();
            expect(() => credits.protectRows(0)).toThrow("already be admitted");
            reach.reach("one prepared slot");
            expect(() => credits.close()).toThrow("steps must settle");
            reach.reach("owner close refuses live step");
            credits.reclaimRows();
            const floor = live + WRITE_ROW_STEP_BYTES + bytes;
            expect(credits.rows.held).toBe(floor);
            reach.reach("reclaim keeps floor");
            const merge = await credits.acquireMerge("prepared-row competing merge", minimum.merge);
            try {
              expect(credits.rows.held).toBeGreaterThanOrEqual(floor);
              expect(merge.allocate(minimum.merge)).toBe(true);
              merge.free(minimum.merge);
            } finally {
              credits.releaseMerge(merge);
            }
            credits.reclaimRows();
            expect(credits.rows.held).toBe(floor);
            reach.reach("merge keeps floor");
            // A metadata transfer may not steal the writer's next step while all other capacity is live.
            const spare = governor.capBytes - governor.reserveBytes - governor.usedBytes;
            holder = await governor.admit({
              label: "competing live root",
              minimumBytes: spare,
              spillable: false,
            });
            expect(holder.allocate(spare)).toBe(true);
            const stop = new AbortController();
            let settled = false;
            const pending = credits
              .growTo(credits.metadata, credits.metadata.held + bytes, stop.signal)
              .then(
                () => {
                  settled = true;
                  return null;
                },
                (error: unknown) => {
                  settled = true;
                  return error;
                },
              );
            await yieldTask();
            expect(settled).toBe(false);
            expect(credits.rows.held).toBe(floor);
            reach.reach("metadata transfer keeps floor");
            stop.abort(new Error("cancel competing metadata request"));
            expect(await pending).toBeInstanceOf(Error);
            // Primary consumes its paid progress synchronously. No governor admission or growth is needed.
            const held = credits.rows.held;
            if (cancel) {
              credits.cancel(new Error("cancel prepared primary"));
              expect(() => step?.check()).toThrow("cancel prepared primary");
              reach.reach("cancel before consumption");
            } else {
              step.check();
              expect(credits.rows.allocate(bytes)).toBe(true);
              expect(credits.rows.held).toBe(held);
              step.release();
              expect(credits.rows.allocated).toBe(live + bytes);
              credits.rows.free(bytes);
              reach.reach("native allocations survive reservation release");
            }
            step.release();
            step.release();
            expect(() => step?.check()).toThrow();
            reach.reach("idempotent retirement");
            expect(credits.rows.allocated).toBe(live);
            credits.rows.free(live);
            credits.reclaimRows();
            expect(credits.rows.held).toBe(minimum.rows);
            reach.reach("idle floor returned");
          } finally {
            step?.release();
            holder?.close();
            credits.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [128 * KiB, 1, false],
          [512 * KiB, 32 * KiB, true],
        ],
      },
    );
    reach.assertReached();
  },
);
