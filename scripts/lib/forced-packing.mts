/**
 * Forced packing. With MINNOW_FORCE_PACKING=1 the engine packs every segment it can — whatever
 * its size or width, single-column segments included — and a read that finds a packed member
 * outside the engine's index fails instead of finding it from its id. The suites that compare
 * results rather than layouts (SQLLogicTest, the simulators, the differential and conformance
 * suites) run under it to show that packed segments read exactly as unpacked ones, and that
 * every path that reads a member indexed its segment first. `npm run test:packed` runs them.
 */
import { packedSegmentTestHooks } from "../../packages/core/src/engine/packed-members.ts";

export const packingForced = process.env.MINNOW_FORCE_PACKING === "1";

/** Turns forced packing on when the environment asks for it; a test may reset the hooks. */
export function applyForcedPacking(): void {
  if (!packingForced) return;
  packedSegmentTestHooks.forcePacking = true;
  packedSegmentTestHooks.missIsFatal = true;
}

applyForcedPacking();
