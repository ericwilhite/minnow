/** A holder's cancellation is not another execution's admission error. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import { Governor } from "../../memory/governor.js";
import { SortGrants } from "./sort-grants.js";

it.each(seedsFor("aggregate-reclaim-owner-cancel", [0xb4db, 0xb4dc]))(
  "isolates cancelled nested holders but still propagates live spill failures (seed %i)",
  async (seed) => {
    const reach = createReach("aggregate reclaim isolation", [
      "cancelled holder",
      "unrelated admission completes",
      "live spill failure",
      "zero",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 768 << 10, max: 1 << 20 }), async (size) => {
        reach.beginHistory();
        for (const cancel of [false, true]) {
          const governor = new Governor(8 << 20);
          const fixed = await governor.admit({
            label: "sort minimum",
            minimumBytes: size,
            spillable: true,
          });
          const extra = await governor.admit({
            label: "sort growth",
            minimumBytes: 0,
            spillable: true,
          });
          const shared = new SortGrants(fixed, [extra]);
          const grant = shared.fork(size, 0);
          expect(grant.allocate(size)).toBe(true);
          const pinned = await governor.admit({
            label: "pinned other minimum",
            minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
            spillable: false,
          });
          const reader = await governor.admit({
            label: "unrelated read",
            minimumBytes: 0,
            spillable: false,
          });
          let reject: (error: Error) => void = () => {
              throw new Error("No reclaim");
            },
            asked = false;
          const work = new Promise<void>((_, no) => {
            reject = no;
          });
          grant.onReclaimRequest(() => {
            asked = true;
            return work;
          });
          const error = new Error(cancel ? "holder execution cancelled" : "spill write failed");
          const waiting = reader.makeRoom?.(2 * size);
          expect(asked).toBe(true);
          try {
            if (cancel) {
              shared.holdFrees = true;
              grant.free(size);
              shared.close();
              fixed.close();
              reject(error);
              await expect(waiting).resolves.toBe(true);
              reach.reach("cancelled holder");
              reach.reach("unrelated admission completes");
            } else {
              reject(error);
              await expect(waiting).rejects.toBe(error);
              grant.free(size);
              shared.close();
              fixed.close();
              reach.reach("live spill failure");
            }
          } finally {
            shared.close();
            fixed.close();
            extra.close();
            pinned.close();
            reader.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero");
        }
      }),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  },
);
