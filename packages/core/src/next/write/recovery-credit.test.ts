/** log-frame.ts: recovery owns one frame until the scanner returns, not for the session. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_RECOVERY_CREDIT_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { RECOVERY_SCAN_BYTES } from "../format/log-frame.js";
import { yieldTask } from "../task.js";
import { WriteCredits, WRITE_ROW_STEP_BYTES, type PreparedWriteCredit } from "./credits.js";

const MiB = 1 << 20,
  working = 4 * MiB;
for (const seed of seedsFor("next-write-recovery-credit", [47221, 47222]))
  it(`retires a completed scan without stealing live or prepared rows (seed ${String(seed)})`, async () => {
    const reach = createReach("completed write recovery credit", WRITE_RECOVERY_CREDIT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 65536 }),
        fc.boolean(),
        fc.boolean(),
        async (bytes, wide, prepared) => {
          reach.beginHistory();
          const governor = new Governor(16 * MiB),
            minimum = {
              rows: working + RECOVERY_SCAN_BYTES,
              metadata: 65536,
              commits: 65536,
              merge: 65536,
            };
          const credits = await WriteCredits.open(governor, minimum);
          const live = wide ? working + bytes : bytes;
          const buffers: {
            rows?: Uint8Array;
            scan?: Uint8Array;
            reader?: Uint8Array;
            extra?: Uint8Array;
            step?: Uint8Array;
          } = {};
          let holder: Grant | undefined, step: PreparedWriteCredit | undefined;
          let stepBytes = 0;
          try {
            expect(credits.rows.allocate(live + RECOVERY_SCAN_BYTES)).toBe(true);
            buffers.rows = new Uint8Array(live).fill(11);
            buffers.scan = new Uint8Array(RECOVERY_SCAN_BYTES).fill(13);
            await yieldTask();
            expect(buffers.scan.at(-1)).toBe(13);
            reach.reach("scan buffer owned through suspension");
            delete buffers.scan;
            credits.rows.free(RECOVERY_SCAN_BYTES);
            reach.reach("completed scan returned");
            reach.reach(wide ? "live rows above working floor" : "small live rows");
            if (prepared) {
              stepBytes =
                working + RECOVERY_SCAN_BYTES / 2 - credits.rows.allocated - WRITE_ROW_STEP_BYTES;
              step = credits.protectRows(stepBytes);
            } else reach.reach("no prepared row");
            const spare = governor.capBytes - governor.reserveBytes - governor.usedBytes;
            holder = await governor.admit({
              label: "completed-scan reader",
              minimumBytes: spare,
              spillable: true,
            });
            expect(holder.allocate(spare)).toBe(true);
            buffers.reader = new Uint8Array(spare).fill(17);
            expect(holder.tryGrow(1)).toBe(false);
            const returned = credits.retireRowsMinimum(working);
            expect(
              returned,
              "an unused open-only reservation must end with recovery",
            ).toBeGreaterThan(0);
            expect(credits.rows.held).toBeGreaterThanOrEqual(
              credits.rows.allocated + credits.rows.loaned,
            );
            expect(buffers.rows.at(-1)).toBe(11);
            reach.reach("idle scan forecast returned");
            expect(credits.retireRowsMinimum(working)).toBe(0);
            reach.reach("retirement idempotent");
            expect(holder.tryGrow(returned)).toBe(true);
            expect(holder.allocate(returned)).toBe(true);
            buffers.extra = new Uint8Array(returned).fill(19);
            reach.reach("competing reader progresses");
            if (step) {
              step.check();
              expect(
                credits.rows.allocate(stepBytes),
                "completed scan retirement must preserve a prepared row's credit",
              ).toBe(true);
              buffers.step = new Uint8Array(stepBytes).fill(23);
              expect(buffers.step.at(-1)).toBe(23);
              delete buffers.step;
              credits.rows.free(stepBytes);
              step.release();
              step = undefined;
              credits.reclaimRows();
              reach.reach("prepared row preserved");
            }
          } finally {
            step?.release();
            if (buffers.scan) {
              delete buffers.scan;
              credits.rows.free(RECOVERY_SCAN_BYTES);
            }
            if (buffers.step) {
              delete buffers.step;
              credits.rows.free(stepBytes);
            }
            if (buffers.rows) {
              delete buffers.rows;
              credits.rows.free(live);
            }
            delete buffers.reader;
            delete buffers.extra;
            holder?.free(holder.allocated);
            holder?.close();
            credits.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [1, false, false],
          [65536, true, true],
          [32768, false, true],
          [16384, true, false],
        ],
      },
    );
    reach.assertReached();
  });
