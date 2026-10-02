import type { FtsChanges, UniqueKeyChanges } from "./types.js";

/**
 * Past this much commit delta — UNIQUE key changes plus index postings and their row locators —
 * a store prepares the commit a slice at a time (`RecordCore.prepareCommit`) before the one step
 * that must not be split.
 */
export const LARGE_COMMIT_DELTA_UNITS = 16_384;

/** How much delta a commit carries, counted until it is known to be large. */
export function commitDeltaUnits(input: {
  readonly uniqueKeyChanges?: readonly UniqueKeyChanges[];
  readonly ftsChanges?: readonly FtsChanges[];
}): number {
  let units = 0;
  for (const change of input.uniqueKeyChanges ?? []) {
    units += change.keyTokens.length;
    if (units >= LARGE_COMMIT_DELTA_UNITS) return units;
  }
  for (const change of input.ftsChanges ?? []) {
    for (const column of change.columns) {
      for (const posting of column.postings) {
        units += 1 + posting.rowIds.length;
        if (units >= LARGE_COMMIT_DELTA_UNITS) return units;
      }
    }
  }
  return units;
}
