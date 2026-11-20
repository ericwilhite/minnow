/** Governor rules2/4/7: an idle owner's forecasts cannot block every next complete step. */
import fc from "fast-check";
import { expect, it, onTestFinished } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_STEP_CONCURRENCY_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { yieldTask } from "../task.js";
import { WriteCredits } from "./credits.js";

const MiB = 1 << 20,
  KiB = 1 << 10;
for (const seed of seedsFor("next-write-step-concurrency", [47201, 47202]))
  it(`makes bounded progress with writers, a merge and a reader (seed ${String(seed)})`, async () => {
    const reach = createReach("complete write steps", WRITE_STEP_CONCURRENCY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 4 }),
        fc.integer({ min: 1, max: 8 }),
        async (count, units) => {
          reach.beginHistory();
          const governor = new Governor(16 * MiB);
          const owners: WriteCredits[] = [];
          const rows = new Map<WriteCredits, Uint8Array>();
          const releaseRows = (owner: WriteCredits) => {
            const bytes = rows.get(owner);
            if (bytes) {
              expect(bytes.at(-1)).toBe(11);
              rows.delete(owner);
              owner.rows.free(bytes.byteLength);
            }
          };
          const jobs: Array<Promise<void>> = [];
          const stop = new AbortController();
          const ended = new Error("concurrent write history ended");
          const minimum = { rows: MiB, metadata: 64 * KiB, commits: 64 * KiB, merge: 64 * KiB };
          let merger: WriteCredits | undefined, merge: Grant | undefined, reader: Grant | undefined;
          const live: { merge?: Uint8Array; reader?: Uint8Array } = {};
          let completed = 0;
          const releaseReaderAndMerge = () => {
            if (live.reader !== undefined) expect(live.reader.at(-1)).toBe(7);
            if (live.merge !== undefined) expect(live.merge.at(-1)).toBe(9);
            delete live.reader;
            delete live.merge;
            reader?.free(reader.allocated);
            reader?.close();
            reader = undefined;
            if (merge !== undefined && merger !== undefined) {
              merge.free(merge.allocated);
              merger.releaseMerge(merge);
              merge = undefined;
            }
            merger?.close();
            merger = undefined;
          };
          const close = async () => {
            stop.abort(ended);
            for (const owner of owners) owner.cancel(ended);
            await Promise.allSettled(jobs);
            releaseReaderAndMerge();
            for (const owner of owners) {
              releaseRows(owner);
              owner.close();
            }
          };
          onTestFinished(close);
          try {
            for (let i = 0; i < count; i++) {
              const owner = await WriteCredits.open(governor, minimum);
              owner.rows.onReclaimRequest(() => owner.reclaimRows());
              owner.metadata.onReclaimRequest(() => owner.reclaimMetadata());
              await owner.prepareMetadata(minimum.metadata);
              expect(owner.rows.allocate(64 * KiB)).toBe(true);
              rows.set(owner, new Uint8Array(64 * KiB).fill(11));
              owners.push(owner);
            }
            merger = await WriteCredits.open(governor, {
              rows: 0,
              metadata: 0,
              commits: 0,
              merge: MiB,
            });
            merge = await merger.acquireMerge("native concurrent merge", MiB);
            expect(merge.allocate(MiB)).toBe(true);
            live.merge = new Uint8Array(MiB).fill(9);
            reach.reach("merge retained");
            const delta = units * 256 * KiB;
            const available = governor.capBytes - governor.reserveBytes - governor.usedBytes;
            const retained = available - delta / 8;
            expect(retained).toBeGreaterThan(0);
            reader = await governor.admit({
              label: "reader awaits writer publication",
              minimumBytes: retained,
              spillable: false,
            });
            expect(reader.allocate(retained)).toBe(true);
            live.reader = new Uint8Array(retained).fill(7);
            reach.reach("reader retained");
            let admissions = 0;
            const admit = governor.admitGroup.bind(governor);
            let requesting: WriteCredits | undefined;
            governor.admitGroup = (options) => {
              if (options.label.startsWith("complete grow") && requesting) {
                for (const account of [
                  requesting.rows,
                  requesting.metadata,
                  requesting.commits,
                  requesting.merge,
                ])
                  expect(account.held, "unused forecast retained during admission").toBe(
                    account.allocated + account.loaned,
                  );
                reach.reach("only live ownership at growth admission");
              }
              // A microtask recycle must also fail boundedly, before starving task turns.
              if (++admissions > 64) throw new Error("Repeated admission without write progress");
              return admit(options);
            };
            for (const owner of owners) {
              requesting = owner;
              const work = owner
                .growTo(owner.metadata, minimum.metadata + delta, stop.signal)
                .then(async () => {
                  stop.signal.throwIfAborted();
                  expect(owner.metadata.allocate(delta)).toBe(true);
                  const output: { bytes?: Uint8Array } = { bytes: new Uint8Array(delta).fill(3) };
                  try {
                    await yieldTask();
                    expect(output.bytes?.at(-1)).toBe(3);
                  } finally {
                    delete output.bytes;
                    owner.metadata.free(delta);
                  }
                  releaseRows(owner);
                  owner.retireMetadataHeadroom(0);
                  completed++;
                });
              requesting = undefined;
              void work.catch(() => undefined);
              jobs.push(work);
            }
            // Count real scheduler turns. The parent must fail a progress assertion,
            // never consume a120s watchdog or depend on an external cancellation.
            for (let turn = 0; turn < 64 && completed < count; turn++) await yieldTask();
            expect(
              completed,
              "circular wait: idle writer floors prevent every next step",
            ).toBeGreaterThan(0);
            reach.reach("bounded progress");
            expect(completed, "each still-open writer must return its completed step credit").toBe(
              count,
            );
            await Promise.all(jobs);
            reach.reach(count === 2 ? "two owners" : count === 3 ? "three owners" : "four owners");
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
        // Eight original random draws plus the five explicit boundary examples.
        numRuns: 13,
        examples: [
          [2, 1],
          [3, 2],
          [4, 4],
          [2, 8],
          [4, 8],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
