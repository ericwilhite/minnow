/** Governor rules 2/3/7 for write growth with substantial live ownership.
 *
 * Law (memory/governor.ts header, memory/README.md "Liveness argument"): admission waits FIFO
 * while holding nothing; growth never waits for unreclaimable space; only a complete minimum
 * above the cap is a typed 53200 refusal. A write owner that keeps live rows while it waits for
 * a metadata step therefore has to spill or unwind them first, and a step whose retained inputs
 * plus its own bytes exceed the cap must be refused rather than queued forever.
 */
import fc from "fast-check";
import { expect, it, onTestFinished } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  WRITE_LIVE_GROWTH_REACH,
  WRITE_RETAINED_MERGE_REACH,
} from "../../testing/reach.js";
import { Governor, MemoryAdmissionError, type Grant } from "../memory/governor.js";
import { yieldTask } from "../task.js";
import { WriteCredits } from "./credits.js";

const MiB = 1 << 20,
  KiB = 1 << 10;
const minimum = { rows: 64 * KiB, metadata: 64 * KiB, commits: 64 * KiB, merge: 64 * KiB };

for (const seed of seedsFor("next-write-live-growth", [47301, 47302]))
  it(`owners with live rows spill before waiting, so every metadata step completes (seed ${String(seed)})`, async () => {
    const reach = createReach("live rows across growth", WRITE_LIVE_GROWTH_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(16, 32),
        fc.integer({ min: 2, max: 4 }),
        // Live rows and the metadata step as fractions of ordinary capacity.
        fc.integer({ min: 38, max: 45 }),
        fc.integer({ min: 25, max: 40 }),
        async (capMiB, count, livePercent, stepPercent) => {
          reach.beginHistory();
          const governor = new Governor(capMiB * MiB);
          const ordinary = governor.capBytes - governor.reserveBytes;
          const fixed = minimum.metadata + minimum.commits + minimum.merge;
          const live = Math.floor((ordinary * livePercent) / 100 / KiB) * KiB;
          const step = Math.floor((ordinary * stepPercent) / 100 / KiB) * KiB;
          // Each owner's complete step fits alone; no two fit with both owners' rows retained.
          expect(live + step + fixed).toBeLessThanOrEqual(ordinary);
          expect(2 * live + step + 2 * fixed).toBeGreaterThan(ordinary);
          reach.reach("each step fits alone");
          reach.reach("steps cannot fit with peers' retained rows");
          const owners: WriteCredits[] = [];
          const rows = new Map<WriteCredits, Uint8Array>();
          const spilled = new Map<WriteCredits, number>();
          const stop = new AbortController();
          const jobs: Array<Promise<void>> = [];
          let completed = 0;
          const close = async () => {
            stop.abort(new Error("live growth history ended"));
            for (const owner of owners) owner.cancel(stop.signal.reason);
            await Promise.allSettled(jobs);
            for (const owner of owners) {
              const bytes = rows.get(owner);
              if (bytes) owner.rows.free(bytes.byteLength);
              rows.delete(owner);
              owner.close();
            }
          };
          onTestFinished(close);
          try {
            for (let i = 0; i < count; i++) {
              const owner = await WriteCredits.open(governor, minimum);
              owner.rows.onReclaimRequest(() => owner.reclaimRows());
              owner.metadata.onReclaimRequest(() => owner.reclaimMetadata());
              // Spill: the rows leave memory (here a checksum stands for the written run).
              owner.bindLiveRelief(() => {
                const bytes = rows.get(owner);
                if (bytes === undefined) return;
                spilled.set(
                  owner,
                  bytes.reduce((sum, value) => sum + value, 0),
                );
                rows.delete(owner);
                owner.rows.free(bytes.byteLength);
                reach.reach("live rows spilled before waiting");
              });
              owners.push(owner);
            }
            // Live rows are taken while capacity allows, as memtable growth does (never waits).
            for (const owner of owners) {
              if (!owner.rows.tryGrow(live)) break;
              expect(owner.rows.allocate(live)).toBe(true);
              rows.set(owner, new Uint8Array(live).fill(11));
            }
            expect(rows.size).toBeGreaterThanOrEqual(2);
            reach.reach(rows.size === count ? "every owner keeps rows" : "some owners keep rows");
            let waits = 0;
            let requesting: WriteCredits | undefined;
            const admit = governor.admitGroup.bind(governor);
            governor.admitGroup = (options) => {
              if (options.label.startsWith("complete grow")) {
                waits++;
                if (requesting !== undefined)
                  expect(rows.has(requesting), "growth waits while keeping live rows").toBe(false);
              }
              if (waits > 64) throw new Error("Repeated admission without write progress");
              return admit(options);
            };
            for (const owner of owners) {
              // Growth reaches its admission synchronously when relief is synchronous.
              requesting = owner;
              const growth = owner.growTo(owner.metadata, step, stop.signal);
              requesting = undefined;
              const work = growth.then(async () => {
                stop.signal.throwIfAborted();
                expect(owner.metadata.allocate(step)).toBe(true);
                const output = new Uint8Array(step).fill(3);
                await yieldTask();
                expect(output.at(-1)).toBe(3);
                owner.metadata.free(step);
                owner.retireMetadataHeadroom(0);
                const bytes = rows.get(owner);
                if (bytes !== undefined) {
                  expect(bytes.at(-1)).toBe(11);
                  rows.delete(owner);
                  owner.rows.free(bytes.byteLength);
                }
                owner.reclaimIdleForecasts();
                owner.notifyReclaimable();
                completed++;
              });
              void work.catch(() => undefined);
              jobs.push(work);
            }
            for (let turn = 0; turn < 64 && completed < count; turn++) await yieldTask();
            expect(completed, "circular wait: owners keep live rows while growing").toBe(count);
            await Promise.all(jobs);
            for (const [, sum] of spilled) expect(sum).toBe(11 * live);
            reach.reach(count === 2 ? "two owners" : count === 3 ? "three owners" : "four owners");
            reach.reach(waits > 0 ? "growth waited" : "growth funded at once");
            reach.reach("all owners complete");
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          } finally {
            await close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          // The reviewed scenario: 16 MiB cap, two owners, about 6 MiB rows and 5 MiB steps.
          [16, 2, 43, 36],
          [16, 4, 38, 25],
          [32, 3, 45, 40],
          [32, 2, 38, 40],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-write-retained-merge", [47303, 47304]))
  it(`a merge counts retained metadata in its complete minimum (seed ${String(seed)})`, async () => {
    const reach = createReach("retained merge minimum", WRITE_RETAINED_MERGE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(8, 16, 32),
        // Required metadata and the merge step, in sixteenths of the cap.
        fc.integer({ min: 1, max: 4 }),
        fc.integer({ min: 8, max: 16 }),
        async (capMiB, retainedSixteenths, mergeSixteenths) => {
          reach.beginHistory();
          const governor = new Governor(capMiB * MiB);
          const credits = await WriteCredits.open(governor, {
            rows: 0,
            metadata: 0,
            commits: 0,
            merge: 0,
          });
          const retained = (governor.capBytes * retainedSixteenths) / 16;
          const bytes = (governor.capBytes * mergeSixteenths) / 16;
          let merge: Grant | undefined;
          let buffer: Uint8Array | undefined;
          try {
            await credits.prepareMetadata(retained);
            expect(credits.metadata.allocate(retained)).toBe(true);
            buffer = new Uint8Array(retained).fill(5);
            // The allocation consumed its prepaid step; only the live bytes stay required.
            credits.retireMetadataHeadroom(0);
            expect(credits.metadata.held).toBe(retained);
            reach.reach("required metadata retained");
            const settled = credits.acquireMerge("merge with retained inputs", bytes).then(
              (grant) => ({ grant }),
              (error: unknown) => ({ error }),
            );
            let outcome: { grant: Grant } | { error: unknown } | undefined;
            void settled.then((value) => (outcome = value));
            for (let turn = 0; turn < 64 && outcome === undefined; turn++) await yieldTask();
            if (retained + bytes > governor.capBytes) {
              expect(outcome, "an unservable merge must not wait").toBeDefined();
              expect(outcome && "error" in outcome && outcome.error).toBeInstanceOf(
                MemoryAdmissionError,
              );
              expect((outcome as { error: MemoryAdmissionError }).error.sqlState).toBe("53200");
              expect(credits.hasMergeStep).toBe(false);
              reach.reach(
                bytes === governor.capBytes ? "merge alone fills the cap" : "merge alone fits",
              );
            } else {
              expect(outcome && "grant" in outcome).toBe(true);
              merge = (outcome as { grant: Grant }).grant;
              expect(merge.held).toBe(bytes);
              expect(merge.allocate(bytes)).toBe(true);
              reach.reach(
                retained + bytes === governor.capBytes ? "exact whole cap" : "below whole cap",
              );
            }
            expect(buffer.at(-1)).toBe(5);
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          } finally {
            if (merge) {
              merge.free(merge.allocated);
              credits.releaseMerge(merge);
            }
            // eslint-disable-next-line no-useless-assignment -- Drop the buffer before returning its credit.
            buffer = undefined;
            credits.metadata.free(credits.metadata.allocated);
            credits.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          // The reviewed scenario: 1 MiB required metadata and an 8 MiB merge at an 8 MiB cap.
          [8, 2, 16],
          [8, 2, 14],
          [16, 4, 12],
          [32, 1, 15],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
