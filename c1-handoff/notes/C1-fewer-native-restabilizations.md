# Fewer native-step re-stabilizations — design note

C1, 2026-11-22; re-scoped 2026-11-23. Design only. Base: main 47e7bf7e, plain `AbortSignal.any`
(the step-signal work was withdrawn from #55; this note does not depend on it).

## What happens now

A native root funds each producer (source sizing, metadata, rules, parameters) through
`NativeRootCredits.#fund` (`owner/native-root-credits.ts`):
- **If no role needs new credit and a source is retained,** it returns at once.
- **Otherwise it opens a native write step** (`statement.prepareNativeStep`, so
  `NativeWriteSteps.prepare` and then the engine's `#stabilizeNativeStepOnce`, `write/engine.ts`).
  The step re-checks the writer's minimum for the current state and catalog, its protected bytes
  plus the root against the capacity, metadata readiness and loans, and may reclaim and relieve.
  Then it opens a boundary, and the pass checks and releases it.
- **The comment says why:** "Between producers, restabilize the writer even when the root needs
  no new credit". A producer may have grown the writer (rows, metadata, a merge), and the next
  producer must not run beside an unproven writer.

**Measured per updated row of the profiled trigger UPDATE** (2,000 rows, one keyed AFTER UPDATE
body; scratch counters on main):

| | Per row |
|---|---|
| funding passes | 13.03 |
| passes needing new credit | 4.04 |
| passes needing none, which still stabilize | 8.99 |
| stabilizations | 14.05 |
| stabilizations finding the writer exactly as the previous one left it | **7.93** |
| stabilizations finding it changed | 6.12 |

"Exactly as left" means the same writer state object, the same `#protectedNativeBytes()`, the same
root bytes, metadata ready, and no loans. The re-stabilizations cost about 1 ms per row (root
funding was about 1.0 s of the 10.5 s profile, the native step 0.42 s of it), plus the
`AbortSignal.any` composites each native step and funding pass creates (`write/native-step.ts`
and `owner/native-root-credits.ts`: two per skipped pass, 26 per row in all).

## Proposal: skip a stabilization the writer can prove is current

1. **A cheap proof.** The engine records, on every successful stabilization for an owner, a
   **stabilization witness**: the state object, the catalog, the minimum state, the protected
   bytes, the root bytes and capacity it proved, the native steps' version, and the credits'
   loan and merge state. New API: `NativeWriteSteps.current(owner, rootBytes): boolean`. It is
   true only when every component is identical and no relief, flush, merge, FTS preparation or
   catalog change has happened since (each already bumps `#nativeSteps` or changes `#state`).
2. **The skip:** `#fund` takes the early return when `missing === 0` and
   `statement.nativeStepCurrent(total)`, even without a retained source. No boundary, no step
   signal.
3. **Everything else unchanged:**
   - a pass that needs credit always stabilizes;
   - the first pass of a statement always stabilizes;
   - the statement-end pass (`prepareEnd`) always stabilizes.

**Why this is safe:**
- A stabilization proves `protected + root ≤ capacity` for a given writer state.
- If none of the inputs to that proof has changed, the proof still holds, and the next producer
  runs beside exactly the writer the previous one ran beside.
- The witness must include everything `#stabilizeNativeStepOnce` reads. The note's
  implementation step is to list them from the code and assert, in a test build, that a skipped
  stabilization would have returned at once with nothing changed.

**Expected:** 7.9 of 14.0 stabilizations per row skipped, about 0.5 ms per row, and 8 fewer step
composites per row (about 16 of the 57).

## Risks

- **A missing witness component** would let a producer run beside a writer that grew. The guard
  is a test-build check: on every skip, run the full stabilization in a dry mode and assert it
  would not change anything. The existing generated suites (native boundary pressure,
  `referential-pressure`, `writer-holder-growth`, the native body suites) then exercise it.
- **Hold-and-wait** (GOVERNOR-WAIT-POLICY): a skip never waits, so it can't introduce a wait.

## Tests and evidence

- **The count:** stabilizations per updated row from 14.05 to about 6.1 on the trigger UPDATE,
  as an assertion, with RED today.
- **The dry-mode check** (test builds) on every skip, across the full next/ suite.
- **Mutants:**
  - a witness missing the protected bytes: a pressure case where a producer grows rows must fail
    (a new targeted test: two producers with rows appended between them at a tight cap);
  - skipping when credit is needed: the existing native boundary tests must fail.
- **Throughput:** the trigger UPDATE at 100, 1k, 2k and 20k rows against main and #55.

## Measurement before building

On main 47e7bf7e, re-run the scratch counters (funding passes, stabilizations, unchanged
stabilizations per updated row): #56 changed write-side funding, so the 7.93 / 14.05 split
may have moved. Build only if most stabilizations per row are still unchanged.

## Size

About 1.5 days: the witness and API (0.5), the skip and the dry-mode check (0.5), and tests and
measurement (0.5). No stored-format change.
