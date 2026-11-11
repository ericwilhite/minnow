/** Grant-or-spill: retaining a hash state may wait on reclaim, never on a pinned minimum. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import { HashHeadroomError, HashWorkspace } from "./hash-workspace.js";
import { ReservedGrant } from "../admission.js";
import { StepLoans } from "../../memory/step-loans.js";
import { Governor } from "../../memory/governor.js";

it.each(seedsFor("next-hash-boundary-growth", [0xb4b1, 0xb4b2]))(
  "funds boundaries, spills under pinned pressure and cancels reclaim waits (seed %i)",
  async (seed) => {
    const reach = createReach("hash boundary growth", [
      "funded growth",
      "pinned pressure declines",
      "reclaim wait",
      "cancel",
      "sync guard",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 4096, max: 32768 }), async (size) => {
        reach.beginHistory();
        for (const mode of ["release", "cancel", "pinned"] as const) {
          const governor = new Governor(8 << 20),
            grant = await governor.admit({ label: "hash", minimumBytes: size, spillable: true }),
            holder = await governor.admit({
              label: "competitor",
              minimumBytes: 0,
              spillable: true,
            }),
            workspace = new HashWorkspace(grant, size),
            controller = new AbortController();
          expect(workspace.allocate(size)).toBe(true);
          const heldBefore = grant.held,
            usedBefore = governor.usedBytes;
          expect(() => workspace.allocate(1)).toThrow(HashHeadroomError);
          expect(grant.held).toBe(heldBefore);
          expect(governor.usedBytes).toBe(usedBefore);
          const remaining = governor.capBytes - governor.reserveBytes - governor.usedBytes;
          expect(holder.allocate(remaining)).toBe(true);
          expect(() => workspace.allocate(1)).toThrow(HashHeadroomError);
          reach.reach("sync guard");
          let release: () => void = () => {
            throw new Error("No reclaim pending");
          };
          const gate = new Promise<void>((resolve) => {
            release = resolve;
          });
          let asked = false;
          if (mode !== "pinned")
            holder.onReclaimRequest(async () => {
              asked = true;
              await gate;
              holder.free(remaining);
              holder.release(remaining);
            });
          const waiting = Promise.resolve(workspace.prepare(2 * size, controller.signal));
          let settled = false;
          void waiting.then(
            () => {
              settled = true;
            },
            () => {
              settled = true;
            },
          );
          await Promise.resolve();
          if (mode === "pinned") {
            expect(await waiting).toBe(false);
            expect(asked).toBe(false);
            reach.reach("pinned pressure declines");
          } else {
            expect(asked).toBe(true);
            expect(settled).toBe(false);
            reach.reach("reclaim wait");
            if (mode === "cancel") {
              const reason = new Error("cancel boundary growth");
              controller.abort(reason);
              await expect(waiting).rejects.toBe(reason);
              release();
              reach.reach("cancel");
            } else {
              release();
              expect(await waiting).toBe(true);
              expect(workspace.allocate(size)).toBe(true);
              workspace.free(size);
              reach.reach("funded growth");
            }
            await gate;
            await Promise.resolve();
          }
          workspace.free(size);
          workspace.close();
          holder.close();
          grant.close();
          expect(governor.usedBytes).toBe(0);
          reach.reach("released");
        }
      }),
      { seed, numRuns: 10 },
    );
    reach.assertReached();
  },
);

it.each(seedsFor("next-hash-private-headroom", [0xb4d5, 0xb4d6]))(
  "does not fund retained hash growth from an idle private read lane (seed %i)",
  async (seed) => {
    const reach = createReach("hash private headroom", [
      "private excluded",
      "pinned declines",
      "released growth",
      "reclaimed ordinary excludes private",
      "zero",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 8192, max: 65536 }), async (size) => {
        reach.beginHistory();
        const governor = new Governor(8 << 20);
        const parts = await governor.admitGroup({
          label: "hash workspace",
          includesProgress: true,
          grants: [
            { label: "fixed", minimumBytes: size, spillable: false },
            { label: "growth", minimumBytes: 0, spillable: true },
            { label: "private", minimumBytes: size * 2, spillable: false },
          ],
        });
        const [fixed, extra, progress] = parts;
        if (!fixed || !extra || !progress) throw new Error("Missing native part");
        const grant = new ReservedGrant(
          fixed,
          extra,
          size,
          new StepLoans(progress, size * 2, "private"),
          false,
          () => progress.close(),
          true,
        );
        const workspace = new HashWorkspace(
          grant,
          size,
          () => grant.held - size * 2 - grant.loaned,
        );
        const holder = await governor.admit({
          label: "pinned",
          minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
          spillable: false,
        });
        try {
          workspace.allocate(size);
          expect(grant.held).toBeGreaterThanOrEqual(3 * size);
          expect(() => workspace.allocate(1)).toThrow(HashHeadroomError);
          reach.reach("private excluded");
          expect(await workspace.prepare(2 * size)).toBe(false);
          reach.reach("pinned declines");
          holder.close();
          expect(await workspace.prepare(2 * size)).toBe(true);
          expect(workspace.allocate(size)).toBe(true);
          workspace.free(size);
          reach.reach("released growth");
          // Reclaim can return earlier optional headroom between boundaries. The
          // old ceiling and an idle private lane do not authorize spending it again.
          extra.release(extra.held - extra.allocated);
          const competitor = await governor.admit({
            label: "later pinned holder",
            minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
            spillable: false,
          });
          try {
            expect(await workspace.prepare(2 * size)).toBe(false);
          } finally {
            competitor.close();
          }
          reach.reach("reclaimed ordinary excludes private");
          workspace.free(size);
          workspace.close();
        } finally {
          holder.close();
          grant.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("zero");
      }),
      { seed, numRuns: 10 },
    );
    reach.assertReached();
  },
);
