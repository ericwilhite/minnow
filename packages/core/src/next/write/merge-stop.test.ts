/** R23: the stop callback is honored at its first requested task slice, without more rows. */
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { createReach, MERGE_STOP_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { UNCHARGED, need } from "./bytes.js";
import { DEFAULT_MERGE_POLICY, MergeAbortedError, mergeRuns } from "./merge.js";
import { encodeRun, RunEncoder, RunReader, runRef } from "./runs.js";
import { Harness } from "./test-support.js";
const reach = createReach("merge stop at slice", MERGE_STOP_STATES);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-write-merge-stop", [0xb509]))
  it(`does no work after the stop slice (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(1, 17, 256), async (slice) => {
        reach.beginHistory();
        reach.reach(
          slice === 1
            ? "single entry slice"
            : slice === 17
              ? "partial block slice"
              : "clock chunk slice",
        );
        const h = new Harness("strict", "memory");
        await h.open();
        const writer = h.space.beginWrite("stop slice");
        const grant = await h.governor.admit({
          label: "stop slice",
          minimumBytes: 8 * 1024 * 1024,
          spillable: false,
        });
        const encoded = need(
          encodeRun([
            {
              key: Uint8Array.of(1),
              entries: Array.from({ length: 1024 }, (_, rowId) => ({ rowId, tombstone: false })),
            },
          ]),
        );
        let output: Awaited<ReturnType<typeof mergeRuns>> | undefined;
        try {
          const ref = runRef(encoded, (await writer.write(encoded.parts)).blobs);
          const reader = await RunReader.open(h.space, ref, UNCHARGED, UNCHARGED);
          let rows = 0,
            stops = 0;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- called below with its encoder receiver.
          const add = RunEncoder.prototype.add;
          vi.spyOn(RunEncoder.prototype, "add").mockImplementation(function (
            this: RunEncoder,
            ...args: Parameters<RunEncoder["add"]>
          ) {
            rows++;
            return add.call(this, ...args);
          });
          let failure: unknown;
          try {
            output = await mergeRuns({
              space: h.space,
              writer,
              grant,
              outputBudget: grant,
              readers: [reader],
              level: 1,
              bottom: true,
              policy: { ...DEFAULT_MERGE_POLICY, sliceEntries: slice },
              stopped: () => {
                stops++;
                return true;
              },
            });
          } catch (error) {
            failure = error;
          }
          output?.release();
          expect(failure).toBeInstanceOf(MergeAbortedError);
          expect(stops).toBe(1);
          reach.reach("slice callback");
          expect(rows).toBe(slice);
          reach.reach("no entries after stop");
          expect(grant.allocated).toBe(0);
          reach.reach("working charge returned");
        } finally {
          vi.restoreAllMocks();
          output?.release();
          encoded.release();
          await writer.abort();
          grant.close();
          await h.close();
        }
        expect(h.governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 12 },
    );
  }, 120_000);
