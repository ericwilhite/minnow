/** Test-only exclusive/recovered portable-load target; production owners supply this seam. */
import type { Grant } from "../memory/governor.js";
import type { ExtentSpace } from "../storage/extents.js";
import { listedRootExtents, reachableExtents } from "../format/reachable.js";
import { openExtentSpace } from "../storage/space.js";
import type { SnapshotLoadTarget } from "./snapshot-load.js";
import type { Harness } from "./test-support.js";
export async function snapshotTestTarget(
  h: Harness,
  currentLsn = 0,
  empty = true,
  observe?: (space: ExtentSpace) => void,
  prepaid?: Grant,
): Promise<SnapshotLoadTarget> {
  const lease = await h.backend.acquire(),
    session = await h.backend.open(lease),
    startup =
      prepaid ??
      (await h.governor.admit({
        label: "load startup",
        minimumBytes: 8 << 20,
        spillable: true,
      }));
  try {
    const space = await openExtentSpace(session, {
      reachable: listedRootExtents(),
      walk: reachableExtents(),
      verifyAtOpen: true,
      budget: startup,
      registerCache: (cache) => h.governor.registerCache(cache),
    });
    observe?.(space);
    return {
      space,
      epoch: session.epoch,
      currentLsn,
      empty,
      async close() {
        try {
          await space.close();
        } finally {
          if (prepaid === undefined) startup.close();
          try {
            await session.close();
          } finally {
            await lease.release();
          }
        }
      },
    };
  } catch (error) {
    if (prepaid === undefined) startup.close();
    await session.close();
    await lease.release();
    throw error;
  }
}
