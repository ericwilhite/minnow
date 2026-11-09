/** Prepaid scalar pair fast paths must not create one scheduler turn or view per in-frame pair.
 * Authority: ExtentPairs.next/ExtentPairWriter.append's synchronous return contracts and R23.5.
 * Actual native I/O and shared-clock boundaries still suspend; all ownership checks remain.
 */
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PAIR_PROGRESS_REACH } from "../../testing/reach.js";
import { TaskClock, taskClock } from "../task.js";
import {
  MetadataJournal,
  MetadataWorkspace,
  METADATA_WORKSPACE_BYTES,
  METADATA_PAYLOAD_BYTES,
} from "./metadata-journal.js";
import { mergeExtents, openPairs, spoolExtents } from "./metadata-extents.js";
import { validateRetirements, validateIncoming } from "./publication-validation.js";
import { countingBudget } from "./space-test-support.js";
import { ScriptedMetadataPages } from "./metadata-test-support.js";
import type { ExtentRef } from "./extents.js";
const reach = createReach("next-pair-progress", PAIR_PROGRESS_REACH);
// Freeze both the explicit stream clock and journal's worker clock: this check counts
// resolved-I/O continuations, not arbitrary timer/host load. Clock/cancellation suites
// independently exercise real later-task boundaries.
const frozen = vi.spyOn(taskClock, "now").mockReturnValue(0);
taskClock.begin();
afterAll(() => {
  frozen.mockRestore();
  taskClock.begin();
});
const SLAB = 1 << 20;
async function turns<T>(
  run: () => Promise<T>,
): Promise<{ value: T; count: number; views: number }> {
  let active = true,
    count = 0,
    views = 0;
  const OriginalView = DataView;
  vi.stubGlobal(
    "DataView",
    new Proxy(OriginalView, {
      construct(target, args) {
        views++;
        return Reflect.construct(target, args) as DataView;
      },
    }),
  );
  const tick = () => {
    if (active && count < 200000) {
      count++;
      queueMicrotask(tick);
    }
  };
  queueMicrotask(tick);
  try {
    const value = await run();
    return { value, count, views };
  } finally {
    active = false;
    vi.unstubAllGlobals();
  }
}
for (const seed of seedsFor("next-pair-progress", [27941, 27942, 27943, 27944]))
  it(`pair work suspends by page/slice, not by scalar (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 512, max: 8192 }),
        fc.integer({ min: 1, max: 7 }),
        async (count, stride) => {
          reach.beginHistory();
          const budget = countingBudget(METADATA_WORKSPACE_BYTES + SLAB);
          const workspace = MetadataWorkspace.create(budget);
          expect(budget.allocate(SLAB)).toBe(true);
          const io = { slab: new Uint8Array(SLAB) },
            journal = new MetadataJournal(new ScriptedMetadataPages(), workspace);
          const clock = new TaskClock(() => 0);
          clock.begin();
          const work = { clock };
          const all = Array.from({ length: count }, (_, k) => ({
            offset: k * 8192,
            length: 1 + (k % 4096),
          }));
          const current = all.filter((_, k) => k % stride !== 0),
            owners = all.filter((_, k) => k % stride === 0);
          const unused = { offset: (count + 1) * 8192, length: 19 };
          const handles: number[] = [];
          const save = async (xs: ExtentRef[]) => {
            const h = await workspace.run("spool", () => spoolExtents(journal, xs, 1, work));
            handles.push(h);
            return h;
          };
          const read = async (h: number) =>
            workspace.run("spool", async () => {
              const p = await openPairs(journal, h, 1, true);
              const out: ExtentRef[] = [];
              try {
                while (await p.next(work)) out.push({ offset: p.offset, length: p.length });
                return out;
              } finally {
                await p.close();
              }
            });
          const bound =
            256 +
            64 *
              Math.ceil(
                ((all.length + current.length + owners.length + 1) * 16) / METADATA_PAYLOAD_BYTES,
              ) +
            16 * Math.ceil(count / 128);
          try {
            const a = await save(current),
              b = await save(owners),
              whole = await save(all),
              supplied = await save([...owners, unused]);
            for (const mode of ["union", "difference"] as const) {
              const result = await turns(() =>
                workspace.run("merge", () => mergeExtents(journal, whole, b, mode, work)),
              );
              handles.push(result.value);
              expect(await read(result.value)).toEqual(mode === "union" ? all : current);
              expect(result.count, `${mode}: scalar fast paths must stay synchronous`).toBeLessThan(
                bound,
              );
              expect(result.views, `${mode}: frame views must be reused`).toBeLessThan(bound);
              reach.reach(mode);
            }
            const retired = await turns(() => validateRetirements(journal, whole, a, b, io, work));
            expect(retired.value).toBe(owners.length * 4096);
            expect(
              retired.count,
              "retirement scalar fast paths must stay synchronous",
            ).toBeLessThan(bound);
            expect(retired.views, "retirement frame views must be reused").toBeLessThan(bound);
            reach.reach("retirements");
            const incoming = await turns(() =>
              validateIncoming(journal, a, whole, supplied, io, work),
            );
            handles.push(incoming.value);
            expect(await read(incoming.value)).toEqual([unused]);
            expect(incoming.count, "incoming scalar fast paths must stay synchronous").toBeLessThan(
              bound,
            );
            expect(incoming.views, "incoming frame views must be reused").toBeLessThan(bound);
            reach.reach("frame view reuse");
            reach.reach("incoming");
            reach.reach("unused supplied output");
            if (count * 16 > METADATA_PAYLOAD_BYTES) reach.reach("page boundary");
          } finally {
            for (const h of handles) await workspace.run("spool", () => journal.release(h));
            workspace.close();
            budget.free(SLAB);
          }
          expect(budget.used).toBe(0);
          reach.reach("exact cleanup");
        },
      ),
      { seed, numRuns: 8 },
    );
  });
it("reached pair progress states", () => reach.assertReached());
