/**
 * Give each original history of a seeded, unconditional async property its own test body.
 * Every body regenerates the SAME stream with the original seed, numRuns and examples;
 * only its ordinal runs expensive work. Across all bodies every history runs exactly once.
 * Create this gate inside each it(), and keep reach.beginHistory() inside the gated work.
 * After failure, run every shrink candidate: routing must never conceal a smaller failure.
 * These callers have no fc.pre or filtering inside their predicates.
 * Test-only: the package excludes *.test-support.* from its published files.
 */
export function historyBody(ordinal: number): (work: () => Promise<void>) => Promise<void> {
  if (!Number.isSafeInteger(ordinal) || ordinal < 0)
    throw new RangeError("A history ordinal must be a nonnegative safe integer");
  let next = 0;
  let failed = false;
  return async (work) => {
    if (!failed && next++ !== ordinal) return;
    try {
      await work();
    } catch (error) {
      failed = true;
      throw error;
    }
  };
}
