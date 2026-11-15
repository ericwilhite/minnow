/** Whole-root claims do not relax R8 exactness, ownership, cancellation or pin safety. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_RETIRE_CURRENT_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { openExtentSpace } from "../storage/space.js";
import { jsonReachable, rootOf, bytesOf } from "../storage/space-test-support.js";
for (const seed of seedsFor("next-retire-current-root", [0xb26d, 0xb26e]))
  it(`proves replacement claims without weakening R8 seed ${String(seed)}`, async () => {
    const reach = createReach("whole root retirement", SNAPSHOT_RETIRE_CURRENT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 8 }), async (count) => {
        reach.beginHistory();
        const backend = new MemoryBackend(),
          lease = await backend.acquire(),
          session = await backend.open(lease),
          governor = new Governor(32 << 20),
          grant = await governor.admit({
            label: "whole root",
            minimumBytes: 8 << 20,
            spillable: false,
          });
        const space = await openExtentSpace(session, {
          reachable: jsonReachable,
          budget: grant,
          registerCache: (c) => governor.registerCache(c),
        });
        try {
          const writer = space.beginWrite("old", { budget: grant }),
            blobs = [];
          for (let n = 0; n < count; n++)
            blobs.push(...(await writer.write([bytesOf(32, n)])).blobs);
          const root = rootOf(blobs);
          await space.publish(root, { writers: [writer], retired: [], budget: grant });
          await expect(
            space.publish(rootOf(blobs.slice(0, 1)), {
              writers: [],
              retired: [],
              retireCurrentRoot: true,
              budget: grant,
            }),
          ).rejects.toThrow();
          expect(space.root).toEqual(root);
          expect(space.failed).toBe(null);
          reach.reach("sharing refused");
          const first = blobs[0];
          if (first === undefined) throw new Error("missing fixture extent");
          await expect(
            space.publish(rootOf([]), {
              writers: [],
              retired: [first.extent],
              retireCurrentRoot: true,
              budget: grant,
            }),
          ).rejects.toThrow("cannot be combined");
          reach.reach("mixed claims refused");
          const cancel = new AbortController();
          cancel.abort(new Error("cancel replacement"));
          await expect(
            space.publish(rootOf([]), {
              writers: [],
              retired: [],
              retireCurrentRoot: true,
              budget: grant,
              signal: cancel.signal,
            }),
          ).rejects.toThrow("cancel replacement");
          expect(space.root).toEqual(root);
          reach.reach("cancel intact");
          const pin = space.pin("old generation", grant);
          try {
            await space.publish(rootOf([]), {
              writers: [],
              retired: [],
              retireCurrentRoot: true,
              budget: grant,
            });
            await space.publish(rootOf([]), {
              writers: [],
              retired: [],
              retireCurrentRoot: true,
              budget: grant,
            });
            const read = await space.read([first], { budget: grant, retain: false });
            try {
              expect(read.bytes[0]).toEqual(bytesOf(32, 0));
            } finally {
              read.release();
            }
            reach.reach("pin survives replacement");
          } finally {
            pin.release();
          }
          await space.audit();
          reach.reach("exact retired inventory");
        } finally {
          await space.close();
          grant.close();
          await session.close();
          await lease.release();
        }
        expect(governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 60000);
