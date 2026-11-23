# Step signals: withdrawn from #55, kept for a later design

Step signals replaced per-row `AbortSignal.any` composites on the native write path with plain
controllers fanned out from long-lived parents (`packages/core/src/next/step-signal.ts` on this
branch). On a 2,000-row trigger UPDATE they cut composites from 57 to 0.01 per updated row and
about 12% of the statement's time. Five review rounds (Astra) each found a way a step differed
from `AbortSignal.any`. The lead withdrew the change from #55 on 2026-11-23: exact cancellation
semantics matter more than that gain, and `AbortSignal.any` gives them for free.

**Why it is structural.** Native "signal abort" marks every dependent aborted, with its reason,
before any listener of the source runs. A fan-out runs as a listener, so between a source's abort
and its fan-out a step reads unaborted where a composite reads aborted. Only a native composite per
step (the cost being removed) is marked in that phase. A prototype with a hidden composite per step
was exact but about 13% slower.

**The rounds** (probe scripts and output here; they import the branch's `step-signal.ts` through
the reviewer's checkout path, `./review/packages/...`, adjust when rerunning):

1. `round1/`: a reentrant abort replaced the caller's reason (single-phase fan-out). Fixed with a
   sentinel per parent and a first-source-wins rule.
2. `round2/`: flattening lost an aborted step's recorded reason; a direct abort of an
   `abortableStep` inside a parent's abort replaced the caller's reason; a close inside a parent's
   abort left the step unaborted. Fixed (recursive "pending", deferrals).
3. `round3/`: a step opened over a step whose abort two nested sources decide fired an extra event
   that could abort an independent step. Unorderable without a composite per step: refused with an
   invariant error, and a suite-wide guard showed engine code never opens such a step.
4. `round4/`: an observer reading a step's `aborted` inside the parent's own listener sees it
   unaborted (native: aborted), so its decision differs ("pending read final state"); a parent
   with both native composites and step signals dispatches them in a different order, so a final
   reason differs ("mixed events", "mixed final reason"). Both follow from the structural gap above.

**On this branch:** the implementation (`step-signal.ts`), its tests (`testing/step-signal.test.ts`:
a fast-check differential against native composites with reentrant listeners, steps opened in
listeners and over aborted steps, direct aborts, throwing listeners and closes in dispatch; the
per-row composite and sentinel counts; prompt cancellation at ten engine sites), the dispatch guard
(`testing/step-signal-checks.setup.ts`) and the converted engine sites.

**A design worth trying later:** native composites created lazily, only for steps that are observed
(read or listened to) while a parent's abort could be in dispatch, keeping plain controllers for the
rest.
