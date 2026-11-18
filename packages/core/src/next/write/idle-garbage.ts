import type { ExtentSpace } from "../storage/extents.js";

/** R10's idle publication predicate also runs while unrelated writers unwind. */
export function idleGarbageOverBound(space: Pick<ExtentSpace, "stats">, floor: number): boolean {
  const stats = space.stats;
  return stats.waitingBytes > Math.max(floor, 0.05 * stats.dataBytes);
}
