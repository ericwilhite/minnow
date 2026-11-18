/** Constructor reservation is independent of affected-row count; refusal precedes buffers. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FOLLOW_UNIQUE_ALLOCATION_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemoryCatalog } from "../lower/memory-catalog.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { RegistryDirectory, REGISTRY_DIRECTORY_BYTES } from "./registry-directory.js";
import {
  NativeFollowUnique,
  nativeFollowUniqueBytes,
  type NativeFollowUniqueScope,
} from "./follow-unique.js";
for (const seed of seedsFor("next-follow-unique-allocation", [0xb709, 0xb70a]))
  it(`charges fixed admitted frames before construction seed ${String(seed)}`, async () => {
    const reach = createReach("follow unique allocation", FOLLOW_UNIQUE_ALLOCATION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 3 }), fc.boolean(), async (frames, deny) => {
        reach.beginHistory();
        const gov = new Governor(8 << 20),
          signal = new AbortController().signal,
          directoryGrant = await gov.admit({
            label: "directory",
            minimumBytes: REGISTRY_DIRECTORY_BYTES,
            spillable: false,
          }),
          grant = await gov.admit({
            label: "follow groups",
            minimumBytes: nativeFollowUniqueBytes(frames),
            spillable: false,
          }),
          spill = new MemorySpillService(),
          directory = new RegistryDirectory(directoryGrant, spill, 1, signal),
          requests: number[] = [];
        const unused = (): never => {
          throw Error("allocation fixture does not execute row/RI callbacks");
        };
        const scope: NativeFollowUniqueScope = {
          rootId: 1,
          frames,
          catalog: new MemoryCatalog().view,
          directory,
          following: { group: unused, require: unused },
          targets: { rows: unused, resolve: unused },
          checks: { verifyUnique: unused },
          charge: {
            allocate: (n) => {
              requests.push(n);
              return deny ? false : grant.allocate(n);
            },
            free: (n) => grant.free(n),
          },
          signal,
          checkpoint: () => undefined,
        };
        try {
          if (deny) {
            expect(() => new NativeFollowUnique(scope)).toThrow(RangeError);
            expect(grant.allocated).toBe(0);
            reach.reach("denied before buffers");
          } else {
            const groups = new NativeFollowUnique(scope);
            expect(grant.allocated).toBe(nativeFollowUniqueBytes(frames));
            await groups.close();
            await groups.close();
            expect(grant.allocated).toBe(0);
            reach.reach("granted and released once");
          }
          expect(requests).toEqual([nativeFollowUniqueBytes(frames)]);
          reach.reach(`frames${String(frames)}` as "frames1" | "frames2" | "frames3");
        } finally {
          await directory.close();
          directoryGrant.close();
          grant.close();
          expect(gov.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
        }
        reach.reach("zero native charge");
      }),
      {
        seed,
        numRuns: 12,
        examples: [
          [1, true],
          [2, false],
          [3, false],
        ],
      },
    );
    reach.assertReached();
  });
