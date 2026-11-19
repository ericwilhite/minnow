/** Governor rule 3 through the real write path: a background merge's complete minimum
 * includes the cold run readers it opened and keeps. A merge that fits the cap alone but not
 * with those retained inputs is a typed 53200 refusal, never an unbounded wait; the write
 * path stays healthy and the inputs stay readable. A merge that fits still runs.
 */
import fc from "fast-check";
import { expect, it, onTestFinished, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_RETAINED_MERGE_ENGINE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { yieldTask } from "../task.js";
import { WriteCredits } from "../write/credits.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rowsOf,
  tableInput,
} from "../write/test-support.js";

const MiB = 1 << 20;

for (const seed of seedsFor("next-write-retained-merge-engine", [47305, 47306]))
  it(`refuses a merge that cannot fit beside its retained readers (seed ${String(seed)})`, async () => {
    const reach = createReach("retained merge readers", WRITE_RETAINED_MERGE_ENGINE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(32, 64, 128),
        fc.integer({ min: 2, max: 4 }),
        fc.constantFrom("oversized", "ordinary"),
        async (capMiB, parts, mode) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory", 2, undefined, {
            merges: { mode: "manual", fanIn: 2 },
          });
          Object.defineProperty(h, "governor", { value: new Governor(capMiB * MiB) });
          let retainedAtMerge = -1,
            stepAtMerge = 0,
            requested = 0;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to the native owner below.
          const acquire = WriteCredits.prototype.acquireMerge;
          const acquired = vi
            .spyOn(WriteCredits.prototype, "acquireMerge")
            .mockImplementation(function (this: WriteCredits, label, bytes) {
              retainedAtMerge = this.retainedBytes;
              // An intrinsically large merge whose own deficit (its bytes plus the next
              // metadata step) fits the whole cap, while the deficit plus the readers and
              // other live bytes it keeps does not. Growth surrenders the prepaid metadata step before it waits and asks for it
              // again with the merge, so the deficit is the merge plus that step.
              stepAtMerge = this.metadataHeadroom;
              requested =
                mode === "oversized"
                  ? h.governor.capBytes - stepAtMerge - Math.floor(retainedAtMerge / 2)
                  : bytes;
              return acquire.call(this, label, requested);
            });
          let closed = false;
          const close = async () => {
            acquired.mockRestore();
            if (!closed) {
              closed = true;
              await h.close();
            }
          };
          onTestFinished(close);
          try {
            let path = await h.open();
            await autocommit(path, (s) =>
              s.changeCatalog({
                kind: "createTable",
                table: tableInput("retained_merge", [column("id", INT, false)], {
                  primaryKey: [0],
                }),
              }),
            );
            for (let part = 0; part <= parts; part++) {
              await autocommit(path, (s) =>
                s.table(1).insert(batch(ints([part * 2, part * 2 + 1])), null),
              );
              await path.quiesce();
            }
            // Cold readers: the merge opens and keeps them before its own admission.
            await h.reopen();
            path = h.path;
            const state = { complete: false };
            const pending = path.runMaintenance().then(() => {
              state.complete = true;
            });
            void pending.catch(() => undefined);
            for (let turn = 0; turn < 64 && !state.complete; turn++) await yieldTask();
            expect(state.complete, "an unservable merge must not wait").toBe(true);
            await pending;
            expect(retainedAtMerge, "merge readers retained at admission").toBeGreaterThan(0);
            reach.reach("merge readers retained at admission");
            expect(path.failure).toBeNull();
            if (mode === "oversized") {
              // The deficit alone fits; with the retained readers it never can.
              expect(requested + stepAtMerge).toBeLessThanOrEqual(h.governor.capBytes);
              expect(requested + stepAtMerge + retainedAtMerge).toBeGreaterThan(
                h.governor.capBytes,
              );
              expect(path.maintenance.merges).toBe(0);
              expect(path.maintenance.mergeFailures).toBe(2);
              expect(path.maintenance.lastMergeFailure).toMatch(/complete grow/);
              reach.reach("oversized merge refused with 53200");
            } else {
              expect(path.maintenance.merges).toBeGreaterThan(0);
              expect(path.maintenance.mergeFailures).toBe(0);
              reach.reach("fitting merge completes");
            }
            // The path keeps writing after a refused merge.
            await autocommit(path, (s) => s.table(1).insert(batch(ints([1000])), null));
            const pin = path.pin();
            try {
              expect(await rowsOf(pin, 1)).toEqual([
                ...Array.from({ length: (parts + 1) * 2 }, (_, id) => [id]),
                [1000],
              ]);
              reach.reach("exact rows after maintenance");
            } finally {
              pin.release();
            }
            reach.reach(
              capMiB === 32 ? "cap 32 MiB" : capMiB === 64 ? "cap 64 MiB" : "cap 128 MiB",
            );
          } finally {
            await close();
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 10,
        examples: [
          [32, 2, "oversized"],
          [64, 3, "ordinary"],
          [128, 4, "oversized"],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
