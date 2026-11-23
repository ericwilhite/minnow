# C1 checkpoint (correctness, completeness, cutover) — rewritten 2026-11-21

Rules: scratch, TMPDIR and logs under /home/eric/.cache (TMPDIR=/home/eric/.cache/c1-tmp); never /tmp.
Stop only my own processes, by PIDs recorded at launch ($! into /home/eric/.cache/c1-tmp/fl4.pids);
never pgrep/ps patterns. Never merge, never push to main, never publish. Push with
`git push --no-verify origin <branch>`. New PRs: `gh pr create --draft`; the lead marks ready.
Every commit: Law/RED/GREEN/Mutant/Reach/Limits; run tsc, eslint, dependency-rules.test.ts
(packages/core/src/next/dependency-rules.test.ts), api:check, version:check. Cross-layer tests go in
next/testing/. Never relax targets, seeds, run counts or assertions to pass.

## Merged
- #11 triggers + governor (holder priority, lifetime registration, stall guard) — merged.
- #34 writer floors step 1 (native step holds no merge minimum) — merged as 8db470b0.
- #26 OPFS kill diagnostics — merged.

## Open PRs
- #35 c1/insert-batch-split (head 135242152, worktree /home/eric/.cache/c1-wt/bigbatch): typed
  BatchTooLargeError 54000 for write batches > 65,536 rows. Lead verifying.

## Writer floors steps 2–3 — STATUS 2026-11-21 late
Branch c1/writer-floors-23 (worktree /home/eric/.cache/c1-wt/floors), WIP commit d4a7800da rebased on
origin/main c1a577a21 + uncommitted: changelog paragraph, corrected large-lane comments (yml + test).
Commit message ready: /home/eric/.cache/c1-tmp/mutants/commit-msg.txt (amend WIP with it).
Done: crash sweep PASS; bench pairs no regression (/home/eric/.cache/c1-tmp/bench/pairs.jsonl);
floor 11.92→4.30 MiB; trigger perf (floors-measure/trigger-perf-*.txt); RED 7/7 on parent; mutants
(/home/eric/.cache/c1-tmp/mutants/*.log); eslint/prettier/api/version clean.
DONE: draft PR #41 opened (head 5bc87b9be after merging main f0bd3cfc9). Awaiting lead.
guarded.sh PID 2000964) → amend commit, push --no-verify, gh pr create --draft, link PR.
Sizing design for lead: /home/eric/.cache/c1-tmp/abort/sizing-design.md (option A recommended).

## (older) In progress: writer floors steps 2–3 (approved design: /home/eric/.cache/c1-tmp/floors-design.md)
Worktree /home/eric/.cache/c1-wt/floors, branch c1/writer-floors (= old #34 branch, merged). UNCOMMITTED.
MUST move to a new branch from origin/main (e.g. c1/writer-floors-23) before committing; draft PR.
Code done (uncommitted):
- write/credits.ts: nativeProtectedBytes(headroom), nativeRowFloorBytes, nativeCommitBytes; #nativeReady
  without row/commit minimum; reclaimNativeStep() releases to native floors; #growTo native targets
  drop rowMinimum/commits minimum.
- write/engine.ts: #protectedNativeBytes and stabilize `needed`/`mandatory` use the native functions;
  reclaimNativeStep() (no arg).
- Tests updated (laws per approved design): write/native-step.test.ts (no commit window),
  write/native-minimum-boundary.test.ts (need = live rows/commits + metadata step),
  testing/native-command-fifo.test.ts (foreign request sized from free memory at that moment,
  bounded by usableCapacity; mutant holder-priority-off still stalls: checked),
  owner/command-primary.test.ts (growth-cancel width 12–14 MiB > idle forecasts; passes 15/15).
- New tests: write/native-commit-funding.test.ts (commit window funded inside a native step under
  pressure; reach NATIVE_COMMIT_FUNDING_REACH; passes; mutant holdsCredit:false → stall: rejected).
  testing/native-write-floors.test.ts: 0A000 sequence refusal test (passes); unit lane 100–1,000 rows
  vs PGlite (passes at 32 MiB, 137 s; 3–4 growth requests/history); large lane 20,000 rows
  (MINNOW_NATIVE_WRITE_LARGE=1, npm run test:native-write:large, wired into sql-conformance.yml and
  check:release). Reach: NATIVE_WRITE_FLOORS_REACH, NATIVE_WRITE_FLOORS_LARGE_REACH.
  write/crash-sweep-native.test.ts: native-step crash sweep with merges fan-in 2.
Crash sweep native: PASSED 2026-11-21 (542 s).
Open problems:
- (old) Large lane timed out (exit 124 after 3600 s, no test output): likely hang, not slowness
  (insert ~3.3 ms/row, update ~8 ms/row). Investigate with the stall guard / smaller N.
- crash-sweep-native: setup assertion `path.maintenance.merges > 0` reads a reopened path (counter
  resets): test bug. Drop it (levels ≥ 2 already proves merges) and run the sweep.
Remaining for the PR:
1. Fix crash sweep setup; run it.
2. Investigate large lane.
3. Measurements on a quiet machine:
   - write load bench pairs: baseline dist built in /home/eric/.cache/c1-wt/main-agg at 933ebcf5
     (#34 head, step 1). Build candidate dist in floors (npx tsc -b packages/core). Rule:
     scripts/write-bench-pairs.mts (9 pairs, median > IQR, ≥7/9). Bench: scripts/write-load-bench.mts
     modes tx/commits/single. No regression for ordinary statements.
   - native floor before/after: /home/eric/.cache/c1-tmp/floors-measure/make-floor-probe.py
     <root> before|after <outfile> writes zz-native-floor.test.ts (run at MINNOW_TRIGGER_CAP_MIB=32).
     Before (step 1): median protected 11.92 MiB (rows 4.00, commits 6.00, headroom ~1.9). Goal ~2.3 + headroom.
   - trigger throughput next/ vs 0.x at 1k/20k + native root peak for UPDATE/DELETE:
     /home/eric/.cache/c1-tmp/floors-measure/zz-trigger-perf.test.ts (copy into next/testing/).
     Known: 2,000-row UPDATE via triggers at 32 MiB refused: main 46.1 MB, steps 2–3 35.7 MB (root's own).
4. Guarded suites 32 and 64 MiB (scopes as in #11 rounds; plus new tests).
5. Branch from main, commit with evidence (two test fixes keep their laws; large histories moved
   to the conformance lane), push, draft PR.

## Trigger write superlinearity (lead: high priority, after the steps 2–3 PR)
Diagnosis: 2,000-row trigger UPDATE 142 ms/row (100 rows: 8 ms/row), same on main; event loop
blocked (no governor wait). Profile 1,000-row UPDATE: ~27% AbortSignal.any composites
(followCompositeSignal/refreshCompositeSignal), ~20% write/memory-read.ts memtable reads, 16.5% GC.
Queue: (a) microtest AbortSignal.any cost vs prior composites on one source; if confirmed, ONE central
helper (one listener on the long-lived signal fanning out to a per-step controller, removed at step
end) replacing ad hoc AbortSignal.any (34 call sites); test: flat per-step cost/alloc count + mutant.
(b) confirm keyed memtable lookup linear in memtable size; if so bring a design (memtable key index /
sorted runs, O(log n)) BEFORE building. (c) re-measure trigger throughput 100/1k/2k/20k vs 0.x; target
flat per-row cost. Scratch: /home/eric/.cache/c1-tmp/floors-measure/zz-profile-update.test.ts,
zz-scale-probe.test.ts. Launch detached runs as
`setsid sh -c 'echo $$ > pidfile; exec timeout … cmd' &` and record the pidfile content.

### Findings 2026-11-21 (worktree /home/eric/.cache/c1-wt/abort, branch c1/abort-fanout from origin/main 642c7a04f; scratch only)
- AbortSignal.any: NO accumulation in the engine. Node flattens composites; every engine composite has
  ≤9 sources (probe /home/eric/.cache/c1-tmp/abort/sites.txt). Flat per-call cost (~15 µs) for siblings
  (micro.mjs/micro2.mjs); only nested chains grow (not present). The profile share is call COUNT:
  MemoryReads.#during composite per memtable readHint.
- Linear term: exec/plan.ts sizeMain → page/read-bounds.ts boundSource sizes the trigger body's keyed
  source (selectedRow, batchRows 1, rows null, all segments) over the WHOLE memtable segment, one
  readHint per row (15,052 hints / 100 lookups at 100 rows; 242,203 / 400 at 400 rows). Native bodies
  skip lookupSegments (no holdsAcrossAdmission facade: plan.ts:4403), and lookupSegments narrows
  segments, not rows. Memtable key index itself is a hash (write/key-hash.ts KeyDelta.find, O(1)).
- Experiment: zz-size-hack (read-bounds hack: size one row for selectedRow memory) — results in
  /home/eric/.cache/c1-tmp/abort/size-hack.txt. Design to bring to lead before building.
  Scratch edits in abort worktree: page/read-bounds.ts (__boundProbe, __sizeOne hacks), testing/zz-*.

## Queue after steps 2–3 (lead, 2026-11-21)
0. (after trigger superlinearity items) 1. Parity audit (small, automatic): run every SUPPORTED entry of packages/core/sql-feature-matrix.json
   (describes shipped 0.x MinnowDatabase) against next/ via publicOwner; generated report + test that
   fails if a supported 0.x feature is refused on next/ without an entry in a reviewed
   `next-parity-gaps` list. Report sized gap list. Fold in: fix changelog line (rebuild identity:
   "omitted values ... use the owned sequence" — overclaims; SQL path refuses).
2. Sequences for 1.0 (decision: required): volatile per-row nextval kernel (PG evaluation order),
   currval session state, identity/serial defaults, native bodies accepting volatile input, PGlite
   differential + mid-statement crash tests. Executor-only. Refusals today: next/exec/kernels/bindings.ts
   nextval/currval "sequences (come with DML)". Probe results: /home/eric/.cache/c1-tmp/floors-measure/seq-probe.txt.
3. Then regex (c1/regex parked c2f24378, option A resumable matcher), trigger-body bugs
   (EXISTS(SELECT *) XX000; SET col = DEFAULT 42703; JOIN USING 42702 — one PR), flake track
   (opfs/handover.test.ts:479 wall clock; chromium resilience.spec.ts:221; webkit
   sql-differential.spec.ts:48 IDB page closed; Firefox OPFS kill traced occurrence in CI),
   re-ordered by the parity audit.
   FLAKE RANKED ABOVE TEST-ONLY FLAKES (lead 2026-11-21): write/extent-deltas.test.ts:650 "a rejected publish
   aborts its attempt; nothing after it commits" — CI oldest-Node shard 2/4, PR #39, run 44903767880 job
   136844932093, log /home/eric/.cache/ci-logs/pr39-node-2.log ~line 2805. "injected publish refusal" escaped
   (likely 2nd autocommit, whose commit freezes memtable → failing flush). Local 12/12; quiesce between commits
   still passes. QUESTION from contract (README ~1529 "commit() is the irreversible boundary… commits unless
   the write path fails"): can a commit whose log frame is durable report the later FLUSH publish failure? If
   yes → outcome-reporting bug (report success or typed uncertain outcome). If engine right (not durable
   yet) → test accepts both orders (tolerate that failure on 2nd autocommit; assert calls for both
   interleavings). Reproduce under load (parallel workers / delay in group-commit ack) before deciding.
   LOW-PRIORITY FLAKE (lead 2026-11-21): browser-tests/semantic-equivalence.spec.ts:6 "opfs: generated semantic
   values, domains and refusals agree across native RPC" chromium, PR #42, passed on retry. CI run 44913349676
   job 136873634886, log /home/eric/.cache/ci-logs/pr42-chr1.log lines 446–470. semanticRunFailure
   (browser/semantic-cleanup.ts:24): OpfsDatabaseInUseError "semantics-<uuid> is open; close all connections
   before deleting it". Likely cleanup deletes before every connection (native-RPC worker's?) finished closing.
   QUESTION from OPFS store close contract: does close resolve before handles are released? If so,
   close-contract bug.
   LOW-PRIORITY FLAKE (lead 2026-11-22): browser-tests/write-scope-admission.spec.ts:7 "concurrent scopes and
   autocommit load every table and survive reopening" Firefox indexeddb+opfs, 30 s timeouts ("Target page...
   closed"), passed on retry, PR #47 (ORDER BY ordinal only). Run 44955713744 job 137003361328, log
   /home/eric/.cache/ci-logs/pr47-ff2.log lines 744–880. Question: work too large for 30 s on loaded Firefox
   (rescale work/timeout with reason) or real stall in concurrent scope admission (bug)? Use trace artifact.
   NEW FLAKE (lead 2026-11-21, item 9): browser-tests/interaction.spec.ts:193 "opfs: the crash seeded
   interaction plan holds across real tabs" chromium, 0.x engine, passed on retry. CI run 44899148022 job
   136831149801 (PR #39); log /home/eric/.cache/ci-logs/pr39-chromium1.log lines 475–530. Tab-errors
   assertion (~line 355) got one maintenance diagnostic: context "posting delta fold for
   <db>/secondary-index:<id>", OpfsUncertainOutcomeError {method:"updateTable"} "could not be
   acknowledged; the mutation may have committed", from storage/opfs/store.js #onMessage via
   BroadcastChannel. Question: is an uncertain-outcome report from a background posting fold in a crash
   plan an accepted transient (fold retries, index stays correct) or a defect? Answer from the OPFS
   store's contract, not test output. If accepted, isMaintenanceContention
   (src/testing/maintenance-diagnostics.ts) needs TYPED evidence the fold re-converges, not a blanket allow. Trigger perf (3.3 ms/root row) joins the perf queue after parity.

## Findings handed off
- INSERT … SELECT with computed text (`'x' || g::text`) fails "Result budget retained buffers at
  handoff" (exec/result-sink.ts:191) on main c6fd1018 → P1 (urgent). Repro:
  /home/eric/.cache/c1-tmp/floors-measure/zz-handoff-min.test.ts.
- executor-agg-handoff stall was the guard firing early (fixed in #11) → P1 for the exec side.

## Worktrees
- /home/eric/.cache/c1-wt/floors — writer floors (uncommitted steps 2–3).
- /home/eric/.cache/c1-wt/bigbatch — #35.
- /home/eric/.cache/c1-wt/main-agg — spare; detached; holds the step-1 baseline dist build (bench).
- /home/eric/.cache/c1-wt/triggers — old c1/triggers (merged).
- c1/regex parked at c2f24378 (pushed).
- Policy note: /home/eric/.local/share/minnow-plan/GOVERNOR-WAIT-POLICY.md.

## Lead decisions 2026-11-21 (after #41)
- #41 under Astra review; lead marks ready.
- Item 3 APPROVED: design A (per-chunk memtable sizing summaries; /home/eric/.cache/c1-tmp/abort/sizing-design.md).
  Rules: maxima only grow within a chunk (deletes/updates/tombstones never lower); reset only when chunk
  dropped at flush. Check point-lookup bound stays small (1-row read ≈ widest row's columns, not chunk
  totals); report overestimate numbers honestly (e.g. one huge text in a chunk). Tests: property summary
  bound ≥ per-row bound incl. after updates/deletes; flat work-per-lookup at 100/1k/20k; mutant per-row
  sizing back; re-measure 100/1k/2k/20k. One draft PR, stacked on #41 if still open, else main.
  Worktree: /home/eric/.cache/c1-wt/sizing, branch c1/memtable-sizing.
- Then: profile 1,000-row trigger UPDATE with A; top costs + share from trigger-body planning (re-plan per row?).
- Parity audit must EXTEND P1's next/testing/next-parity-gaps.ts (branch test/p1-next-simulator), not fork.
  P1 found UNION ALL refused on next/.

## Lead 2026-11-21: new 1.0 item — rekey key forwards on next/ (queue: after design A + 14 ms/row profile, BEFORE parity audit)
- next/ has no key forwards: after rekey the old key neither reads/writes the row nor counts as taken;
  duplicate insert on a retired key accepted. Promise: apps/site/content/docs/sql/dml.mdx "Changing a
  primary key" (old key reads/writes the row for keyForwardRetentionMs, 10 days default; new key equal to
  a retired key → UniqueConstraintError) + engine/rekeying.mdx (forward table). No 0.x-only caveat.
- Likely stored-format impact → must land before format freeze. DESIGN FIRST to lead: forward storage,
  retention/expiry, key lookups + memtable key index, flush/merge, crash recovery.
- Seeds: P1 branch test/p1-next-simulator, gap `rekey-forwards`.
- P1 note: /home/eric/.local/share/minnow-plan/handoff/P1-to-C1-parity-gaps.md. Parity audit extends
  next/testing/next-parity-gaps.ts (0A000 + message pattern); audit's unlisted-gap check fails on anything
  not listed.
- Owner split: P1 owns UNION ALL / set-operation family and the writer-turn bug (autocommit lands while
  another connection's BEGIN holds writer turn). Don't duplicate.

## Astra review of #41 (lead 2026-11-21): #41 stays draft until fixed. Do after design A.
P1 BLOCKING: commit funding waits while txn rows counted; holder-to-holder FIFO cycle. credits.ts:175
releases commit window; later funding queues behind an earlier holder (32 MiB, lifetime holdings:
100-row INSERT w/ trigger at commit holds 187,044 row bytes; earlier holder asks 25,248,318 on top of its
1 MiB, fits usable 26,297,918 only if writer rows released). Autocommit passes on main, fails on #41.
Repro: /home/eric/.cache/astra-pr41/source/packages/core/src/next/testing/review-commit-fifo.test.ts; logs
/home/eric/.cache/astra-pr41/sql-fifo.log, sql-fifo-base.log. Adopt repro as RED (tidied).
Law: no hold-and-wait incl. counted held bytes under FIFO; commit must never wait.
Direction: fund commit window when nothing is held — autocommit: with statement's first admission;
explicit txn: with first write's admission, kept for whole txn (not per native step). Better design → lead first.
Related pre-existing: two-statement txn blocks same way on main (stmt 2 admission waits holding stmt 1 rows):
fix here if same mechanism, else Limits + size estimate.
Tests: autocommit + multi-statement repros; mutant: commit funded at commit time → rejected by ASSERTION.
P2 GATE: remove 20k lane from sql-conformance.yml:41 and check:release (package.json:54) in #41; re-enable in
design-A PR with evidence the lane completes.
Astra clean otherwise (charge-before-alloc OK, test laws OK, floor mutants by assertion).

## Design A status (worktree /home/eric/.cache/c1-wt/sizing, branch c1/memtable-sizing on c1/writer-floors-23) — UNCOMMITTED
Code: write/memtable.ts (valueSummary, charged per-chunk max/sum Float64Arrays; never lowered; reset on
dropped chunk; null for rows not appended e.g. recovered staged), write/memory-read.ts (summary over a run
of chunks), write/view.ts (source.summary + readSummary), access/snapshot.ts (MemorySummaryRequest,
MemoryReadSummary, MemorySegment.readSummary?), exec/late-fetch.ts (forward, no call spread),
page/read-bounds.ts (one summary per sized source when batchRows===1, else per-row walk).
ALSO write/stream-body.ts effects(): O(undo²) "already emitted" rescans → per-segment Sets (42% of 20k
trigger INSERT CPU). Changelog entry added (design A only so far; add effects line).
Tests: testing/memory-sizing-summary.test.ts (seeds 52011/52012; MEMORY_SIZING_SUMMARY_REACH; passes),
testing/memory-sizing-work.test.ts (253 hints + 100 summaries per 50 lookups at 100/1k/20k; passes),
write/commit-effects-linear.test.ts (36.7 reads/entry flat; mutant 684 → rejected).
Mutants rejected by assertion: per-row back (130.6/lookup), rollback lowers, missing→0, first chunk only.
Scratch in tree to delete before commit: testing/zz-trigger-perf.test.ts, testing/zz-profile-insert.test.ts.
Measurements: /home/eric/.cache/c1-tmp/sizing-run/ (trigger-perf.txt, profiles). With A only: UPDATE
9.7/10.8/12.8/69.9 ms/row at 100/1k/2k/20k; INSERT 6.2/3.5/3.4/9.9. Re-measuring with effects fix (perf2 PID 2075394).
Large lane did NOT finish in 3600 s (loaded, before effects fix).
#41 EXTRA REGRESSIONS (pass on main f0bd3cfc9, fail on #41 head): testing/referential-pressure,
body-follow-nested-boundary (ReadStepBoundaryError lease crossed downstream memory request),
native-write-step-borrow ("promise resolved instead of rejecting"), native-write-step (ReadDeniedError 27
bytes), referential-follow-groups (120 s timeouts). My #41 GREEN skipped most of next/testing. Fix with the
Astra P1 commit-funding work. public-worker-boot fails in sizing tree only because dist not built.

## 2026-11-21 ~09:45 status
DESIGN A (sizing worktree): 20k lane PASSED in 373 s. Bound numbers: one wide col identical; two wide cols in
different rows value bound 120,044 vs 60,045, total 22,868,968 vs 22,759,952 (+0.48%). Re-measure (A+effects):
INSERT 7.04/3.20/3.37/3.11 ms/row (100/1k/2k/20k), UPDATE 10.96/8.49/11.25/13.68*, DELETE 5.89/6.86/7.94/11.56*
(*20k overlapped another run). 0.x 20k: UPDATE 12.8, DELETE 30.4 ms/row. Remaining mild term: memtable
deletedUnder O(deletions) per lookup (1.5%→7.7% of DELETE CPU at 2k→12k). Gates clean (eslint, prettier,
api:check, version:check). Full next/ suite running (PID 2099355, log sizing-run/suites2.log); inherited
#41 failures only so far. Still TODO: commit, push, draft PR (stacked on #41), re-enable lane (it's inherited
from #41 branch; #41 must drop it).
#41 FIX (floors worktree, uncommitted on c1/writer-floors-23):
- credits.ts: #transactionCommit pin (holdCommitWindow, transactionCommitBytes, #commitFloor; growTo exit
  requires commits ≥ pin; prepareMetadata fast path too); engine.ts begin → holdCommitWindow(true), release →
  false. Test testing/native-commit-no-wait.test.ts (Astra repro tidied; autocommit + two-statement armed at
  commit). RED on 5bc87b9be: both fail by assertion. GREEN 3/3. Mutant (no pin) rejected by assertion.
- engine.ts #runReader: RunReader.open page reads charged to metadata budget (was rows grant → ReadDeniedError
  27 bytes in testing/native-write-step "grow"). native-write-step now passes. #bitmap still reads via rows
  grant (hazard; readBlob needs Grant) — unproven, note in Limits.
- With pin: referential-pressure + referential-follow-groups pass. Remaining: body-follow-nested-boundary
  (fixture command-root.test-support.ts ensure() uses makeRoom while holding → ReadStepBoundaryError;
  production NativeRootCredits uses admitGroup holdsCredit → align fixture), native-write-step-borrow (self
  case rootBytes from pre-reclaim protected → now served; law fix like fifo).
- P2: remove large lane from sql-conformance.yml + check:release in #41.
Pre-existing findings (also on main f0bd3cfc9): earlier holder needing committed memtable bytes never served
(idle writer doesn't flush for holder growth?); ParseBudgetError seen once in two-statement variant (maybe
spill-over from timed-out case) — verify.
- Tried aligning fixture ensure() to holder growth (admitGroup holdsCredit + transfer): body-follow-nested
  then HANGS (2×120 s) → genuine wait cycle under #41 floors in that scenario. Reverted fixture. Needs analysis.

## Lead decisions 2026-11-21 ~10:00 (QUEUE)
1. Design A PR ON MAIN (39c34682), not on #41. Check on main: whole next/ suite; clean re-measure
   100/1k/2k/20k (nothing overlapping); 20k lane. Re-enable lane (sql-conformance.yml + check:release) in
   this PR. effects() fix = SEPARATE COMMIT with own evidence (RED linearity test, rescan mutant, 42% profile).
   Draft PR; lead gets review.
2. Then holder-never-served (main governor-law bug; changelog "idle writer flushes when another request
   needs their memory") + #41 cycle: run production-style holder-growth fixture on MAIN for
   body-follow-nested-boundary; bring cycle (who holds/waits) BEFORE fixing. Verify ParseBudgetError cleanly
   (untyped = bug; should be 53200 or wait).
3. Finish #41: no failing/hanging tests, no new cycle. native-write-step-borrow: fix TEST to law (must-refuse
   exceeds usable cap after reclaim: cap − reserve − lifetime − unreclaimable), cite law. Pull lane from #41.
   Whole next/ suite before re-push.
4. 14 ms/row profile incl deletedUnder. 5. rekey design. 6. parity audit. 7. flakes.

## Design A PR OPENED 2026-11-21: #44 (draft), branch c1/memtable-sizing-main, head 79d1ecb59
Commits d270c48f3 (summaries), 62daf5189 (effects linear), 442f3ce7c (lane), merge of origin/main 154c85eba.
Evidence: full next/ suite 1106/1106 (before merge); clean re-measure (trigger-perf-main.txt); lane 241 s;
RED on main 130.56/lookup and 684 reads/entry; mutants all by assertion. Summaries lazily charged on first
append; freed at release() (publication) → re-pointed readers size row by row (arena-pool law).
Old branch c1/memtable-sizing (#41-based WIP 2136b92ed) is superseded; worktree /home/eric/.cache/c1-wt/sizing now on -main.
NEXT: item 2 holder-never-served (repro /home/eric/.cache/c1-tmp/holder/zz-holder-served.test.ts → run on main)
+ #41 cycle (production-style fixture on MAIN for body-follow-nested-boundary; bring the cycle before fixing).

## Item 2 findings 2026-11-21 ~13:30 (reported to lead; no fix built)
- "Holder never served" is NOT a governor bug: request sized usable−base−1KiB; commit grew lifetime holdings
  by ~3 KB → request (≤ usable) lawfully waits for the test's own base grant. With 64 KiB margin all 3
  variants served on main (incl. holder growth queued during txn). Idle residue measured 23,744 B
  (lifetime growth, not reclaimable by design). ParseBudgetError NOT reproduced in 3 clean runs (spill-over).
- Pin (#41 floors worktree) makes Astra's ORIGINAL test pass (autocommit + multi), 2/2.
- body-follow-nested-boundary with production-style fixture (/home/eric/.cache/c1-tmp/holder/command-root.prodstyle.ts)
  STALLS ON MAIN TOO (pre-existing). Stall snapshot: waiter root growth 2,512,844 B holdsCredit; free 1,048,576;
  writer rows 4,194,304 held/480,268 alloc, metadata 1,848,320/1,098, commits 6,291,456/1,049,652; root
  metadata 3,975,893/2,135,218, source 110,388/0; competitor 38,466,039 (held until statement completes).
  → statement waits for memory only its own writer's protected idle floors or the competitor (which waits
  for the statement) can supply. Fixture lacks NativeRootCredits' prepareNativeStep-before-growth.

## Lead decisions 2026-11-21 ~13:45 (Astra reviewing #44)
ORDER: 1) LOAN PR on main (pre-existing hazard), 2) #41 rebased on it + fixture (prepareNativeStep reclaim
then holder growth + transfer; authority NativeRootCredits.#fund; verify main stalls / #41 serves) +
native-write-step-borrow law fix + lane pulled + full next/ suite, 3) 14 ms/row profile (check per-row body
re-planning first; next/ 3–9× slower than 0.x at 100–2k), 4) rekey-forwards design.
LOAN RULES (option a, scoped): (1) statement root borrows only its OWN statement's writer idle credit above
writer allocated, via transferCredit; (2) for one root step only — returned before writer's next step and
before commit; (3) loan outstanding at writer step/commit = invariant error, thrown + tested; (4) never
across a wait: wait as holder growth only for remainder, holding nothing borrowed; if loan+free < need →
return loan, wait, re-borrow; (5) commit pin loanable (returned before commit).
Tests: RED cycle repro on main (prod-style funding + competitor) fails by stall guard typed report; GREEN
served; mutants: no loan (stall), loan kept across writer next step (invariant), loan held across wait
(hold-and-wait check fires). Own PR on main; then rebase #41.

## Loan PR status 2026-11-21 ~15:00 — BLOCKED on design; reported to lead
Worktree /home/eric/.cache/c1-wt/loan (branch c1/root-loan from main 154c85eba), UNCOMMITTED:
- testing/command-root.test-support.ts now funds via production NativeRootCredits (open/reserve/prepareSource,
  rules bound to credits.rules, useStatement() redirect, releaseIdleStep no-op once aborted);
  native-trigger-cancel.test.ts calls f.useStatement. With it on main: 3 competitor tests stall by stall guard
  (body-follow-nested-boundary, command-root-pressure, native-body-sizing) — RED set; others pass.
- Loan code: access/native-write-step.ts (NativeWriterLoan, NativeLoanTarget, NativeLoanOutstandingError),
  credits.ts (lend/#lendable/#assertNoLoan at prepareNativeMinimum, reclaimNativeStep, prepareCommit,
  protectRows), engine/transaction lendNativeIdle, governor interface canAdmitNow?, native-root-credits.ts
  (#loan, #repay, STEP_ROLES source/rules above base, borrow-first in #fund).
- Stalled request is SOURCE growth 2,512,844 (step-scoped). But invariant shows writer steps/commits happen
  INSIDE root steps (body-host #completeRow → #commitFts → prepareCommit; #prepareMinimum at commit), source
  stays allocated across #fund calls (prefix-images/child-inputs), cancelled roots can't repay → rules
  "repaid before writer's next step" unmet. Also #41+pin+prod fixture still stalls: pinned commit window
  (6.29 MB held, 5.24 idle) is the memory the source step needs.

## Lead decision 2026-11-21 ~15:30: option (a)
PR on main: NativeRootCredits prepays MEMORY_READ_HINT_BYTES + headers in SOURCE role while nothing held,
whenever any trigger body has a read source; ONE named constant derived from hint charge + test prepay ≥ one
hint charge; bounded-stall test (competitor retained, need above prepaid → typed stall error, competitor+engine
usable after); production-funded fixture; fix competitor tests leaving no room for prepaid+pin (cite law).
Evidence: RED 3 stall reports on main; GREEN; mutant prepay w/o hint minimum stalls; measured peak per native
trigger stmt before/after; trigger re-measure 100 & 2,000 rows (shouldn't move). Docs if they state reservations.
(b) later (after #41): WORK only for chunks that can parse a page; property resident read ≤ smaller charge.
ORDER: (a) PR → #41 rebase (+borrow law fix, lane pulled, full suite) → 14 ms/row profile → rekey design → (b)
→ parity audit → flakes.

## (a) prepay status 2026-11-21 ~16:30 (worktree loan, branch c1/root-loan, UNCOMMITTED; loan code dropped)
- write/memory-read.ts: CALL_BYTES, hintCharge(), exported memoryReadSizingBytes(columns) = 1024 + hintCharge(false, 2048, cols).
- owner/native-root-credits.ts: reserve/cover accept source; prepareSource keeps base.source.
- owner/native-command.ts: Want.source, learnedEntry sourceBytes, planReadsTables(), rootSourceBytes(plans, tables)
  (root plan + body plans; prepay when any reads), prepareWithin retry when source short, reserve incl source.
- fixture: production NativeRootCredits + reserve source rootSourceBytes(...).
Result: command-root-pressure PASSES. body-follow-nested-boundary + native-body-sizing still stall: body query
EXECUTION working set (exec/run.ts:909 admitWorkingSet) 933–984 KB beyond prepaid (data-dependent).
Governor.stallAfterMs is TEST-ONLY (null in production) → "waits on us" HANGS in production. Reported to lead.
## Lead decisions ~16:45: production bound = CANCELLABILITY (no new refusal). Dedicated test: non-releasing
competitor + native stmt waiting above prepaid → cancel via signal ends promptly w/ typed cancellation; nothing
leaks (grants returned, pinned window released w/ rollback); competitor+engine usable after, later stmt runs;
test builds: stall guard names waiter+holders. Docs: one sentence (write waits while memory held elsewhere; a
result held open by same app can make it wait until released; cancelling ends the wait) — grep storage/engine
docs + agents rules. Two tests → RELEASABLE competitor (evictable cache or spillable holder releasing on reclaim);
if a test truly needs non-releasing → leave room (prepaid+pin+measured need), check reach; say per test in commit.
Then: prepay ≥ hint test, RED/GREEN/mutant, peak per stmt before/after, re-measure 100 & 2,000, docs.

## 2026-11-21 ~17:30 FINDINGS (reported; awaiting lead) — loan worktree state saved: /home/eric/.cache/c1-tmp/holder/prepay-and-fixture.patch
- Fixture with production's includesProgress: true → the 3 competitor tests PASS with ORIGINAL non-releasing
  competitors; mutant without prepay also passes → prepay (a) NOT needed; stalls were fixture-only
  (includesProgress false). Releasable competitor dropped (tests back to originals).
- Real pre-existing untyped errors under pressure (public path, competitor holds all ordinary but 1 MiB):
  (1) owner/prepaid-query.ts registerCache RangeError "Native query cache metadata must be admitted" →
      fixed in worktree (shared frozen DECLINED_CACHE; no per-query charge). RED on main via public path.
  (2) exec/trigger-bindings.ts:163 BindingScope.take Error "Trigger binding workspace was not admitted" for
      1–2 MiB NEW/OLD values (main too); ≥3 MiB values wait instead. Needs a funding design.
- Cancellation test scenario: 6 MiB value UPDATE with AFTER trigger copying it → genuinely waits.
- Client abort rejects with signal reason (DOMException AbortError), not 57014.
## Lead decisions ~17:45: drop prepay (+ releasable competitor); fixture builds credits from SAME factory/options
as production (RED: 3 stalls flag off; mutant: flag removed). Cancellation: assert DOCUMENTED contract
(engine/errors.mdx:36 cancelled = AbortError from caller's signal; workers.mdx:130 read AbortError; :149
dispatched mutation → unknown-outcome path) for 6 MiB UPDATE; row unchanged (or unknown → resolves consistently
on next read); no leak; competitor+engine usable; stall guard names waiter/holders. Docs sentence in
engine/memory.mdx "Cancelling a query" (~line 60). NEXT PR: binding-workspace bug (size from actual NEW/OLD values
before copy, fund as cancellable root holder growth while holding nothing unreleasable; hold-and-wait case → lead
first; generator value-width dimension small→several MiB under small cap; sweep "was not admitted"/"must be
admitted" throws reachable from public paths, list them). PR ORDER: this PR → binding → #41 → 14ms → rekey → (b)
→ parity → flakes.
- Lead ~18:30: #43 merged, main ee9768bd; lead merged main into c1/memtable-sizing-main → ae29f2c6b (fetch before touching). Base current PR on ee9768bd.

## PR #46 opened (draft) 2026-11-21 ~19:30: c1/root-funding (worktree /home/eric/.cache/c1-wt/loan), head d9010b182
Commits 570133091 (fixture via openRootCredits, useStatement, session funds after opening statement),
9c001b567 (registerCache DECLINED_CACHE + native-body-pressure-public.test.ts + changelog),
b90aed21d (native-wait-cancellation.test.ts + engine/memory.mdx), merge of main ee9768bd.
NEXT: binding-workspace PR — design to lead first: registry-images #read already MEASURES scalars
(registryScalarBytes in #walk) and funds imageExtra+scalarBytes+workspace via scope.prepare (cancellable,
no lease held; comment says "including one unique binding copy") — extend that prepare to cover the
binding copy (sum of binding bytes, same formula as trigger-bindings take) when projection is a binding read;
check BindingScope charge is same account as images' scope.charge. Generator: value-width dimension.
Sweep candidates (data-sized, maybe public-reachable): registry-images.ts:578, registry-scalar.ts:37,
array-data.ts:119, join-keys.ts:227, parameter-checkpoint.ts (40/107/126), trigger-bindings.ts:163.
- Lead ~19:45: #46 reviewed clean, ready, in CI. Binding plan APPROVED: fund copy in same scope.prepare with ONE shared size fn used by take + prepare; generator decides which sweep sites fire → fix those; list unreachable w/ reason in Limits. Then #41.

## Binding-funding PR (worktree /home/eric/.cache/c1-wt/binding, branch c1/binding-funding from main ee9768bd) ~21:00
UNCOMMITTED; commit msg /home/eric/.cache/c1-tmp/pr-c/msg.txt; changelog added.
Code: registry-scalar.ts bindingCopyBytes + BINDING_COPY_OVERHEAD_BYTES; statement-command.ts RowBindingLease.copyBytes;
registry-images.ts measure per side + prepare(bytes + workspace + (copies ? copyBytes : imageExtra));
trigger-bindings.ts take(funded) + invariant. Test testing/native-trigger-value-width.test.ts (seeds 53011/53012,
NATIVE_TRIGGER_VALUE_WIDTH_REACH). RED on main (1 MiB UPDATE + competitor → untyped Error). GREEN 2/2. Mutants:
implicit funding back → rejected; lease bytes unfunded → rejected; overhead 0 → not rejected (slack; documented).
Full suite running (PID 2295264, log holder/bsuite.log). Then: commit, merge main, push, draft PR.
Finding: 6 MiB trigger UPDATE at 64 MiB needs ~75 MB → 53200 on main too (maybe overestimated; unexamined).
NEXT: #41 rebase onto main (after #46 merges ideally): floors worktree has pin (credits/engine), run-reader
metadata fix, native-commit-no-wait test; + fixture from #46 (openRootCredits) + native-write-step-borrow law
fix + pull lane (not on #41 branch anymore? lane was in #41 originally; #44 added it on main) + full suite.
- Lead: #46 merged, main 99d4ce8a; #44 in CI (c04342b99) — fetch before touching. Binding PR on 99d4ce8a.
## PR #48 opened (draft) ~22:15: c1/binding-funding, head bf9a4bcb6 (f91fe2125 + merge of main 99d4ce8a).
Binding read prepare(bytes + workspace + copyBytes); full read unchanged (578 full-read in Limits);
registry-images.test projected bound 2KiB*cols+256 (law). NEXT: #41 rebase onto main (99d4ce8a + maybe #44).
## Lead ~22:30: #48 under Astra review. GO #41 now (drop #41 lane wiring if #44's lane makes it redundant).
Queued after #41, before 14 ms profile: (1) 6 MiB trigger UPDATE needs ~75 MB at 64 MiB (12×): break down each
charge/copies, which are real simultaneous holdings vs overestimates — correctness (never refuse below usable cap);
(2) registry-images.ts:578 full-read lease + array-data.ts:119: add dimensions, fix like #48; for 578 the broken
decode-workspace envelope test is a signal — decide via law whether envelope or funding is wrong first.
## Lead ~23:00 Astra on #48: P2 full image read over-reserves (registry-images.ts:610: imageExtra+scalarBytes+workspace
vs lease bytes=2048*cols+scalarBytes → payload twice) = the 75 MB; Astra scratch (bytes+workspace) lets 6 MiB UPDATE complete.
P2 test: refused.bytes>capacity trusts engine; derive need independently, require completion when it fits usable cap;
6 MiB must complete. Optional: projected-read exact funding + fragments read. New commit on #48: RED/GREEN/mutant
(double formula back) + remeasure 6 MiB reserve. :578 / array-data:119 → "add a dimension later". Then #41.
#41 state (floors worktree): merged main afba5288 (3167afd58); lane dropped (77cdd27b8); borrow law fix (bisection)
passes — committing as WIP.
## 2026-11-22 ~01:00
#48 follow-up pushed 875c096b7 (full read funds bytes+workspace; independent-need test; RED/mutant; 6 MiB UPDATE
completes, peak 40.2 MB; suite 1108/1108). Findings in PR Limits: query sizing over wide rows (12 MiB row read
asks 76.7 MB; 2×6 MiB param predicate 89.3 MB) — same cap law, unexamined.
#41 (floors): run-reader mutant rejected (ReadDeniedError); changelog para updated (uncommitted); full suite running
(PID 2372101, log holder/fsuite.log). Then: commit changelog (msgD), filter-branch msg rewrite with
/home/eric/.cache/c1-tmp/pr-41/msgfilter.sh over origin/c1/writer-floors-23..HEAD, push --no-verify (force needed?
no — rewriting only unpushed commits), update PR #41 body.
## Lead FYI ~01:10: P1 owns #recycleLog post-switch metadata failure (ParseBudgetError → WritePathFailedError);
lead will ask C1 to review that PR's write-path reasoning. Coming from P1: Governor.servableBeside(own, bytes, signal)
— use for native-root and binding-read sites (refuse 53200 when never fits beside own statement's unreleasable
holdings after reclaim); lead will route. Continue #48 (done, pushed 875c096b7) then #41.

## #41 finalization (2026-11-21 19:40)
- First full suite (PID 2372101) HUNG: 4 workers spinning in #growTo (directory, staged-generated, merge,
  modify-upsert), 3 OOM at 4 GB. Cause: #growTo's two inner exits lacked the commit-window check that the
  pin added to the top exit and prepareMetadata's fast path -> engine re-prepare loop spun on resolved promises.
- Fix: shared #grown predicate in credits.ts; new write/transaction-commit-window.test.ts (seeds 53021/53022,
  TRANSACTION_COMMIT_WINDOW_REACH). RED/GREEN/two mutants recorded in msgE.txt.
- WIP messages reworded via commit-tree (NOT filter-branch: it rewrote GitHub-signed main merges).
- Local head after merging origin/main 277803432: see `git log -1` in /home/eric/.cache/c1-wt/floors.
- Full suite rerun: PID 2408097, log /home/eric/.cache/c1-tmp/holder/fsuite2.log. If green: push
  --no-verify, update PR #41 body (pin, growth-exit fix, run-reader, borrow law, lane dropped), link PR.
- 2026-11-21 ~21:10: full suite rerun (growth fix): no hang, 9 fails/4 files. Pin moved begin -> first
  write (local WIP commit, engine #writes at admit/reserveCommandRows/prepareNativeStep/prepareCatalog/
  liveMemtable): fixes wait-wake + write-maintenance-credit. STILL FAILING (pin design conflicts, asked
  lead): write-query-reclaim [52,4,"after",true] (BEGIN;UPDATE;SELECT reader 48.2 MiB can't fit beside
  held window -> self-wait) and writer-holder-growth [20000,100,"update"] (#relieve MemtableMemoryError).
  #41 NOT pushed. Proposed alternative: spill/stage the tx's rows before a commit wait instead of pinning.
- 2026-11-21 lead: #48 items (registry-images :610 double count, independent need) were already done in
  475b12a6b / pushed 875c096b7 — told lead. #41: option 1 design note written:
  handoff/C1-41-stage-before-commit-wait.md (4-6 days, partial; recommends (a) drop pin + point-6 unless a
  real concurrent holder path exists). Waiting for lead decision. Next in queue after: servableBeside
  adoption, 14 ms profile, rekey-forwards design.
- 2026-11-22: #41 local head cd28cc609 (pin dropped + HolderChain invariant). Suite fsuite3 killed ~21:40 (no
  summary; use --maxWorkers=2). Failures: referential-pressure ("prepaid native window must progress with
  competitor retained"), referential-follow-groups + body-follow-nested-boundary (120 s timeouts) = the
  pre-pin #41 regressions back (pin had fixed them); range-cursor "waits at admission" = read snapshot
  range cursor trips invariant (real path: exec/key-order.ts:380 non-admitted ordered scan). Reported to
  lead with option (e): native steps keep the commit window as on main (gain ~3.4 MiB, rows only).
  NOT pushed. #50 merged (main bacda00b); #48 merged main by lead (fetch first).
- 2026-11-22: lead approved (e) + read-cursor new admission. Local head e71a90a43 (merged ed8ad390, #48 in).
  Commits: cd28cc609 (pin drop + invariant), 7d6106d59 ((e) + read cursor + read-cursor-admission test),
  merges. Measured (e): median 8.75 MiB (p95 11.90) vs 11.92 before; rows 0.36. PR body draft:
  /home/eric/.cache/c1-tmp/pr-41/body.md (fill FULL_SUITE_RESULT, BROWSER_RESULT). Full suite PID 2592458
  (fsuite5.log, --maxWorkers=2). Then browser: npm run test:browser:next-owner + library specs
  next-storage-owner-reopen, next-storage, write-scope-admission, interaction. Then push --no-verify, edit PR.
- 2026-11-22: #41 PUSHED f3ab48c3b (draft, on ed8ad390). Full suite green 1120/7518 (3138 s, 2 workers);
  browser next-owner 44 pass (chromium/firefox; webkit cannot launch locally: libicudata.so.74);
  library holder specs 56 pass/34 skip. Body+title updated. Next per lead: servableBeside adoption (wait for
  P1's helper), query sizing over wide rows (76.7 MB read-back), 14 ms profile, rekey-forwards design.
  Also convert stack-string tests (extent-deltas :256,:693, catalog-drop-checkpoint, staged-failure) when touched.
## Wide-row read-back (2026-11-22), worktree /home/eric/.cache/c1-wt/readback (branch c1/wide-row-readback from ed8ad390)
Scratch in tree (never commit): ZZ-PROBE logs in exec/run.ts (#reserveBound) and page/read-bounds.ts; tests
testing/zz-readback-probe.test.ts, zz-readback-p1.test.ts; env MINNOW_ZZ_PARTS=<file>. Logs: c1-tmp/readback/.
Breakdown, SELECT a WHERE id=1 (one 6 MiB memtable value, 64 MiB): total 65.4 MB =
 keyLookup#1/minimum 12.83 (2x output, memory path read-bounds.ts:~398) + keyLookup#1/progress 25.10
 (ReadBound.progress = 4x largestValue, read-bounds.ts:742) + result/progress 24.12 (#stepBytes uses the
 source's read progress again, run.ts #fallbackResultStep) + result window 1.05 + keyLookup#3 2.29.
Also refused below cap: INSERT two 6 MiB params (result/progress lane 4.96 MB vs step 12.36 MB, StepLoans);
UPDATE SET b=$1 6 MiB (11.37 vs 4.97); INSERT 3.25 MiB at 32 MiB cap (MemtableMemoryError 6.82 MB).
- 2026-11-22: #41 Astra round 2 fixed, pushed 6eb049cf6 (ordered scan funds cursor via IndexShape.cursor;
  view rangeCreditHolders check; invariant test bounded; no-aging law stated + counter-case). write/+exec/
  green. Waiting on Astra re-check. Read-sizing WIP in readback worktree commit 8114d879c (fix 3 done,
  scratch probes inside — never push); next: fix 1 (lane funds only uncovered, read-bounds + run.ts lane for
  covered one-row source), fix 2 (enable resultWidth recipe for plain delivery), independent-need test.
- 2026-11-22: #41 Astra round 3 fixed, pushed 8a4f1de4b (read cursors never queue: tryAdmit or
  HolderChainInvariantError; ordered scan cursor sized by legacyCursorPlan resident/planRange; 8-12 MiB
  sweep; range-cursor wait test moved to statement view). Full suite PID 2723406 (fsuite8.log) running.
  Read sizing: readback worktree has fixes 1-3 uncommitted on top of WIP 8114d879c (stepCovered lane,
  plain-delivery result recipe excluding modify). Next: independent-need test (build rows: 1 MiB works at
  32 MiB; probe of larger widths timed out at 600 s — rerun per case), then full suites, then PR.
- 2026-11-22 HANG ON MAIN (ed8ad390): 32 MiB, t(id PK, a TEXT, b TEXT), INSERT (2, 2 MiB, NULL), then
  UPDATE t SET b = a WHERE id = 2 -> query#3/fixed-buffers waits forever (stall guard at 20 s: needs 23.4 MB,
  used 19.7 of 33.55; writer holds rows 4.37 alloc, metadata 1.53 idle, commits 6.29 held/0.13 alloc, merge
  4.73 idle). Statement's own query waits on its writer's idle forecasts. Repro: mainprobe worktree
  testing/zz-readback-build.test.ts with ZZ_CASES=32:2 MINNOW_GOVERNOR_STALL_MS=20000. Reported to lead.
  Also 64 MiB 6 MiB `SET b = a` -> MemtableMemoryError 25.2 MB (typed but below cap?).
- 2026-11-22: #41 merged (main fab73201). Read sizing (readback worktree, uncommitted on WIP 8114d879c):
  fixes 1-3 + stored-path selected rows + testing/wide-row-read-need.test.ts (13 cases; success law + one-row
  admission ≤ independent need). RED on main: 3 fail. Remaining: tiny row beside 6 MiB at 64 MiB admits 30.3 MB
  (need 12.6/18.9): memtable hint residentBytes = MEMORY_READ_RESIDENT_BYTES + GZIP envelope (16 MiB worst
  case per gzip page; PageEntry has no raw length; writer gzips small page bodies even for random values since
  wide value is out-of-line). Real peak 2.78 MB. Asked lead: fix 4 (measure inflation in sizing via counting
  budget, or format field) here or follow-up.
- CORRECTION 2026-11-22: tiny-row 30.3 MB is NOT gzip. memory-read.ts METADATA worst case (~16.2 MB: 1 MiB column
  directory at 26 B/entry parsed at 192 B + root parseCharge(1 MiB)) inside MEMORY_READ_RESIDENT_BYTES, charged
  for an unpublished, non-retained chunk because it may publish before the read opens. Flush writer does not gzip
  memtables (comment at memory-read.ts:64) -> side finding "writer gzips tiny pages" retracted. Proposed fix 4':
  bound the future publication's metadata by the memtable's row count (pages >= 1 row) and column count.
- 2026-11-22: read sizing fixes 1-3 move `SELECT id, a, b FROM t ORDER BY id` (64 MiB, row 3 MiB a + 3 MiB b copy)
  from 53200 (66.25 MB, main) to a HANG: asks 64,041,638 <= usable 64,046,142, but the writer's live idle
  allocations (~286 KB: rows 146 KB, metadata 7.5 KB, commit buffers 132 KB) leave 64,037,596 grantable.
  Same class as #51 (usable cap ignores writer's unreleasable live holdings). Reported to lead.
- 2026-11-22: read sizing draft PR #52 (c1/wide-row-readback, 89f611130 on fab73201). Fixes 1-3 (fix 2 gated
  on over-cap retry after parameter-result-progress reach caught the ungated version). Full suite green
  (1121 files). Fix 4 design note: handoff/C1-fix4-retain-selected-chunk.md (retain selected chunk via
  prepareRead({locator}) at sizing). Pinned: ORDER BY 3+3 MiB @64 MiB gap (#51), stored ORDER BY sort lane (P1).
  Throwaway worktree /home/eric/.cache/c1-wt/mainprobe (detached main) can be removed.
- 2026-11-22: #52 review fix pushed b84768936 (complete admission capture tryAdmit/admitGroup/admit + non-zero;
  stored-path picks restored (stored tiny 38.2 -> 3.58 MB); named allowance terms; stall cause asserted).
  Full suite green 1121. Fix 4 WIP in /home/eric/.cache/c1-wt/fix4 (c1/fix4-retain, on old 89f611130,
  uncommitted: run.ts retainSourceRow, plan.ts oneChunk retention, test allowance removed) — rebase onto
  #52's new head before continuing; memtable allowance terms in the test then shrink.

## 2026-11-22 ~13:30: #52 suite5 RED (rebased 781d015f7 on main 44b14d72), NOT pushed
- suite5: 2 files / 5 tests failed (7561 passed).
- (1) wide-insert-admission seeds 23297/23298: untyped `RangeError: Admitted memory output did not fit`
  on `SELECT v WHERE id = 3`. Cause: memory-scan decoded a selected chunk from row 0, so an index fetch
  materialized a wide neighbour that fix-3 per-row sizing never funded. FIXED in 64e4bbf03
  (memory-scan reads only the live span [first live, last live]; new testing/key-read-neighbours.test.ts;
  memory-scan unit test + changed pin 0/8/16 → 1/9/17 because fixture deletes position 0).
- (2) statement-wait-release (P1's #51 test), 3 cases: precondition "other read holds > 8 MiB" fails
  (7,441,044) because fix 2 sizes that 2 MiB scan correctly (main: 13,732,660). UNDERLYING DEFECT ON MAIN:
  an INSERT admitted beside a 5–9.5 MB holder is refused mid-run, typed 53200
  "query#N/result/progress needs a minimum above the cap (… 177696 available)" from ModifySpool →
  RecursiveTape → SpillIo.need → StepLoans.borrow; law says it must wait. Main repro: same test with a
  1.5 MB row (holds 6.46 MB), or two 2 MiB rows (main fails all 3). Write side = P1. AWAITING LEAD decision
  on what #52 does with this test. Did NOT change the test.
- suite6 (on 64e4bbf03) PID in fl4.pids, log /home/eric/.cache/c1-tmp/readback/suite6.log.
- Stored path checked: SelectedGroup decodes densely only when the range fits the ≤64 KiB fetch target.

## 2026-11-22 ~13:30 Lead decision (a): P1 cuts result/progress lane fix as its own PR (my repros = RED).
- Once it lands: rebase #52 onto it; statement-wait-release setup → two 2 MiB rows; keep 8 MiB assertion.
- #52 local head now 6dfc43af0 (64e4bbf03 amended: README live-span sentence). UNPUSHED.
- Fix 4 committed locally: worktree /home/eric/.cache/c1-wt/fix4, branch c1/fix4-retain, b75b12b2a on 6dfc43af0.
  New test-only hook QueryRuntime.afterReservation; testing/retained-key-read.test.ts (publish after sizing,
  ±cancel; borrowedChunkBytes 1,064,964 lent then 0 before pin release). Mutants: no retention, never release.
  Small-beside-wide 64 MiB: 30,333,162 → 4,626,696. Changelog + README updated. Full suite on fix4 NOT yet run.
  Open fix 4 draft PR only after #52 is reviewed (lead).
- suite6 (6dfc43af0 code): 1,128 files; 7,568 passed, 1 skipped, 3 failed (only statement-wait-release, known); 3,444 s.
- fix4 full suite launched on b75b12b2a, PID 2937541, log /home/eric/.cache/c1-tmp/readback/suite-fix4.log.
- ~15:05: #52 local head 5a492191c (plain held/loan fields; per-instance accessors only when observeHoldings set at
  construction; bench in PR body). fix4 rebased: dc8a655ef. fix4 full suite (b75b12b2a): 7,570 pass, only the 3
  known statement-wait-release. +16 MiB mutant rejected on #52 (stored case) and fix4 (8/14).
- servable adoption: probes (trigger copy/update bodies, 32/64 MiB, widths 0.5–6 MiB, 1/3/6-row txns, fine
  sweep at the cap boundary) found NO hang at native root/binding: served below cap, typed 53200 above
  ("native root and writer" check at exactly the cap). No RED yet → reported to lead.

## ~16:00 #53 merged (main 284f32ccd). #52 rebased: e600533de (6 commits). NOT pushed.
- #53 rewrote statement-wait-release: READERS [2 MiB],[1.5 MB],[2 MiB,2 MiB]; precondition > 4 MiB; law "written at
  once or once read ends". Main 10/10. #52: 4 fail — 1.5 MB reader holds 1,504,944 (< 4 MiB precondition, #52's
  accurate sizing); 2 MiB reader (#52 holds 7.4 MB): insert refused "write path memtable … (3008352 bytes, 563430
  available)" (bytes.ts:65 ← transaction.ts:990). MAIN REPRO 284f32ccd: reader 1,000,000 or 1,200,000 + VALUES
  3,000,000 → "write path memtable … (6008352 bytes, 1065484 available)". Reported to lead.
- ORDER BY pin: already no stall branch (3c0de36cd). suite7 on e600533de running.
- ~16:40 lead (a): #52 waits for P1 PR 2. Committed ee05b047a (swr precondition: reader holds ≥ its rows; law
  assertion waited|written). #52 now 3/10 swr fail (memtable refusal) + wide-row-read-need 32 MiB/3 MiB build fails:
  #53 REGRESSION on main 284f32ccd — plain 3 MiB INSERT VALUES at 32 MiB, nothing else running, refused
  "write path memtable … (6303920 bytes, 951312 available)"; 44b14d725 wrote it. Reported.
- fix4 rebased: b5a3f9e7a on ee05b047a (same 1 failure as #52 from #53). Next: 14 ms/row profile.
- ~18:20 suite7 (e600533de): 7,577 pass, 6 fail = 5 swr (pre-fix) + wide-row-read-need 32/3 MiB (#53 regression).
- 14 ms/row PROFILE (scratch /home/eric/.cache/c1-tmp/profile14: zz-profile-update.test.ts inspector profile,
  top.mjs/byfile.mjs/phase.mjs). 2,000-row trigger UPDATE (1,000 updated), quiet: main 6.13 s, #52 5.19 s (−15%),
  #52+16 registry leaves 4.21 s (−31%). Per updated row on main: registry get 95, put 33, leaf miss 9.6 (8 leaves),
  flush 6 (30 appends each), dir page reads 368, appends 174; AbortSignal.any 57 (13 native-step prepare
  native-step.ts:129, 13 root #fund native-root-credits.ts:225, 12 memory-read.ts:180, 5 registry-images:552…);
  13 native step prepares/row; root funding ≈1.0 s of 10.5 s. Profile shares (main): index fetch ~22% (#52 fixes),
  registry family ~30%, GC+program 11%, root funding ~4%. Reported; proposals await lead.
- ~19:00 lead: #53 3 MiB refusal accepted (PR 2 fixes); profile decisions: 16 leaves APPROVED (named constant,
  derived charge, work-count assertion), abort fan-out helper APPROVED (composites/row assertion, prompt cancel per
  site, no listener leak over 2k rows, mutant), adaptive radix + fewer re-stabilizations = design notes later.
  ORDER: rekey design note → perf PR (1+3, draft, re-measure vs 0.x 100/1k/2k/20k) → notes 2,4; #52/fix4 when PR 2 lands.
- Rekey note written: handoff/C1-rekey-forwards-design.md. KEY: D14 (unanimous) removed forwarding; divergences.ts
  registers it as intended; lead's 11-21 item reverses it → asked lead to choose (A) build ~7–9 d (internal table
  + catalog flag + format bump) or (B) keep D14, docs/test change ~1–2 d.
- ~20:30 lead: D14 KEPT (option B). Draft PR #54 c1/no-key-forwarding (worktree /home/eric/.cache/c1-wt/d14), head
  e77875eca on main 284f32ccd: docs 0.x-marked + 1.0 cutover sections, agent rules, changelog, simulator
  SimulationDriver.keyForwarding (default true; next driver false), finding removed, rekeying reach + seeds 5–12.
  keyForwardRetentionMs: next/ has no public options yet (NativeBootOptions lacks it) → refuse/deprecate decided at
  1.0 public options. Next: perf PR (16 leaves + abort fan-out). Rekey design note kept for a future extension.
- ~20:30 #54 merged (main 6f666866). Perf PR branch c1/trigger-perf (worktree /home/eric/.cache/c1-wt/perf):
  68e474f05 16 leaves (+registry-commands/registry-directory test setup derived), f417a4040 step signals (10 sites,
  57→0.01 composites/row, listener removed when last step closes). Full suite PID 3117276 log
  /home/eric/.cache/c1-tmp/perf/suite.log. Throughput /home/eric/.cache/c1-tmp/perf/throughput.txt. NOT pushed yet.
- ~21:30 perf PR #55 opened (draft): c1/trigger-perf head f417a4040 on main 6f666866; full suite 7,587 pass/1 skip.
  Next: design notes (adaptive radix depth; fewer native-step re-stabilizations); #52/fix4 when P1 PR 2 lands.
- ~22:30 Astra P2 on #55 (reentrant abort reason) fixed locally 0f619c652: two-phase via per-parent sentinel
  AbortSignal.any([parent]) + skip rule + step flattening + abortableStep; fast-check differential vs AbortSignal.any.
  Lead accepted ~4% cost (sentinels 13/row, composites 0.01/row; two assertions). Suite2 PID 3166523 log
  /home/eric/.cache/c1-tmp/perf/suite2.log; then push + PR body (state cost) → Astra re-check.
- QUEUE after #55: design note adaptive registry radix depth; design note fewer native-step re-stabilizations;
  then FOLLOW-UP sentinel reduction (per-call signal creators pass step signals: funding caller, boundary
  invalidated, read/query contexts, checkpoints; measure sentinels/row before/after; differential green).
  #52/fix4 when P1 PR 2 lands.
- ~23:00 #55 pushed 0f619c652 (suite 7,592 pass/1 skip); body updated (cost stated). Awaiting Astra re-check.
  Design notes written: handoff/C1-registry-adaptive-depth.md (path compression; reads ~115→15-20/row, appends
  ~64→10-15) and handoff/C1-fewer-native-restabilizations.md (7.93 of 14.05 stabilizations/row unchanged; witness
  skip; dry-mode test check). Next: sentinel-reduction follow-up after lead's notes decision.
- ~00:30 Astra re-check #55: 2 P2 (flattening lost decided reason; direct abort replaced caller's) fixed in
  65fe0fbe6 (recursive pending, born-aborted single pending source, deferred close/direct abort, abortable stands
  own signal). Extended differential (dynamic opens, abortable direct aborts, throws, closes). KNOWN DIVERGENCE
  pinned (it.fails): step opened over step with 2 nested pending sources → reason exact, event timing differs;
  exactness needs per-step composite (hybrid prototype ~13% slower). Suite3 running; then push + report.
  Radix WIP parked: /home/eric/.cache/c1-wt/radix fd49dd7a6 (has lint error; resume after #55 clean).
- 2026-11-23 ~01:10 #55 pushed 196ed25fe (contract doc, cap, suite-wide check via step-signal-checks.setup.ts).
  Suite4: 7,597 pass, 1 fail = sql-write-batch-size 300 s timeout under load 12-13 (passes alone 89-171 s, not the
  check). Awaiting Astra final check. Then: radix (note 1, WIP fd49dd7a6), note 2, sentinel follow-up.
- 2026-11-23 Astra final #55: P2-a bucket hid final-state divergence; lead's fix (native composite of flattened
  sources) proven inexact (array order) → REFUSE the unorderable open (TypeError), all worlds compared; P2-b setup in
  all next/ projects; P3 inDispatch (armed/running). c6dcda7e7 local, suite5 running. Radix WIP ddedc3f01.
- 2026-11-23 ~03:30 #56 merged (main 47e7bf7e). #55: suite5 green at c6dcda7e7 (7,604); 98f6bf57f invariant error
  (StepSignalInvariantError XX000); rebased onto 47e7bf7e → b5c470ea3 pushed (reach.ts conflict; rerere had a bad
  recorded resolution → forgot + redone). Targeted 82 files pass. #55 full suite on b5c470ea3 PENDING (after #52's).
  #52 rebased → a448925d5 (swr 16/16, wide 14/14), suite8 PID in fl4.pids running. Then push #52, rebase fix4.
- 2026-11-23 ~04:40 LEAD: step signals DROPPED from #55 (5th Astra round: structural). #55 → registry-only
  (perf worktree c1/trigger-perf = 9f8ba017a on 47e7bf7e, README bullet folded in; NOT pushed yet). Step-signal work
  + Astra probes + notes on origin c1/step-signal-wip (45cc6bd06). Body draft /home/eric/.cache/c1-tmp/perf/body-registry.md
  (THROUGHPUT_TABLE + FULL_SUITE_RESULT placeholders). TODO #55: re-measure 100/1k/2k vs main+0.x, full suite, force-push.
  Note 2 rescoped to plain AbortSignal.any.
- #52: suite8 on a448925d5: 3 fail (2 #56 tests relying on read over-reservation → fixed 01720cd69; sql-write-batch-size
  load timeout) + 1 worker heap OOM (file unknown). suite9 (json reporter) PID 3344348 running to identify it.
  Found: main 47e7bf7e stalls on snapshot-pinned-holdings with 3 MB rows (read over-reservation starves metadata grow).
- 2026-11-23 ~05:50 #52 PUSHED 01720cd69 (force-with-lease over b84768936), body rewritten
  (/home/eric/.cache/c1-tmp/readback/body-52.md: span fix, peak holdings, rebase + #56 test setups, main stall).
  suite9: 1 fail = sql-write-batch-size load timeout (alone 162.6 s #52 / 165.5 s main) + 1 worker heap OOM: the
  crash-sweep-native worker (#20's file, last to finish at 570 s; worker died ~2 min later). Alone it passes on
  both (~520 MB flat). Open: is it main's? #55 suite6 (9f8ba017a on 47e7bf7e, --logHeapUsage, wsample.sh >1 GB
  sampler → /home/eric/.cache/c1-tmp/perf/suite6.{log,json,rss}) running; it doubles as #55's required suite.
  Then update #52 body's heap paragraph.
- fix4 rebased locally: c1/fix4-retain 4f8a48a74 on 01720cd69 (tsc ok; waiting tests 38/38; retained-key-read 2/2).
- NEXT: #55 throughput re-measure on a quiet machine (main 47e7bf7e vs 9f8ba017a vs 0.x, 100/1k/2k), fill body,
  force-push --force-with-lease=c1/trigger-perf:b5c470ea3, gh pr edit 55 title+body. Then note 2, note 1 (radix).
- Radix (note 1) REBASED onto registry-only #55: c1/registry-compress 092665714 on 9f8ba017a (old head kept as
  c1/registry-compress-wip-old = ddedc3f01). tsc ok; registry tests 4 files/12 pass. MUTANT done: split losing
  remainder digits (copy loop removed, registry-directory.ts ~360) → compression property fails both seeds
  (49374, 49375) "expected false to be true" (a put record not found); restored. Left: lint, throughput, squash
  + commit message, draft PR after #55.
- ~06:40 Astra P2 on #52 (loan double count in observeHoldings) FIXED f4ff95949 (held-only accessor; new
  memory/observe-holdings.test.ts; RED 3072≠2048; +16 MiB mutant still rejected 20,357,093). PUSHED; body updated.
  Draft → CI skipped; lead marks ready, then CI runs. fix4 rebased → 09c2cda73 on f4ff95949 (18/18).
  Lead: if crash-sweep-native heap growth is main's (suite6 tells), report → lead schedules it separately.
- 2026-11-23 ~08:30 #52 OOM SOLVED: not a cross-file leak (vitest isolate:true → fresh fork per file). File =
  testing/wide-copy-admission.test.ts "copies a 6 MiB value at 64 MiB": UPDATE t SET b = a spun forever in
  microtasks in write/engine.ts #waitForRows (inspector pause via NODE_OPTIONS=--inspect=127.0.0.1:0 +
  /home/eric/.cache/c1-tmp/cdp/pause.mjs). Main 47e7bf7e's spin (reclaim returned the waiting step's rows),
  reached on #52 because fix 1 (stepCovered: run.ts + read-bounds.ts; reverting either → refused typed) lets
  the copy get that far. #57 (main f574465c, P1 b5f5cb06a) fixes exactly that spin. Rebased #52 onto f574465c
  (backup ref c1/wide-row-readback-pre57); wide-copy-admission passes. #57's writable-transaction-read-wait lost
  "refused typed" reach under #52 → f73da111e adds width 3,000,000 + example [32,3M,ORDER BY] (mutant: #57's
  readOnly→null rejected, stall). suite10 (rebased, --logHeapUsage, sampler) running; then push --force-with-lease
  (remote f4ff95949), body update, report. fix4 rebased → 9998bdc30.
- #55 suite6 GREEN (9f8ba017a on 47e7bf7e): 1136/1136 files, 7609 pass/1 skip, no OOM, 4295 s. #55 needs rebase on
  f574465c before push? (registry-only; check conflicts) + throughput re-measure.
- ~09:40 #52 PUSHED 4114b6e3a on main f574465c (force-with-lease over f4ff95949); suite10 1140/1140, 7635 pass,
  no OOM (peak worker 2.24 GB vs 2.35 GB main baseline). Body updated (diagnosis, rebase, reach change). PR is
  READY (lead); lead watches CI. fix4 local d3a6e07fd on 4114b6e3a: draft PR only after #52 merges.
  NEXT: #55 rebase onto f574465c + throughput re-measure (quiet machine) + force-push + title/body.
- ~12:00 #52 CI red (cov 7/7 seed 25345): writable-transaction-read-wait post-rollback check `SELECT id FROM t
  WHERE v='z'` refused (46.35 MB: scan 22.1 + result progress 18.96 + step 4.2 + 1). Fix 2 gap: over-cap retry
  gated on resultSources.size>0, so fixed-width-only results kept the 4-copy envelope. c964ac205 (plan.ts guard
  `|| !aggregateResult`; wide-row-read-need key-only scan cases 1/2/2.5 MB at 32 MiB stored). 20 extra seeds pass.
  suite11 running; then gates, push --force-with-lease (remote 4114b6e3a), body (body-52.md updated), report.
  fix4 → rebased on c964ac205.
- ~13:30 #52 PUSHED 3cd9b1279 on main 7f0a6277f (#58 merged; rebase clean; pre-#58 ref c1/wide-row-readback-pre58).
  suite11 on c964ac205 (pre-#58): 1140/1140, 7638 pass. Rebased head: 248 files/2065 tests targeted (incl. #58's).
  Body updated. Lead watches CI. fix4 → d7c61c01d on 3cd9b1279 (local; PR after #52 merges).
  NEXT: #55 (rebase onto main, throughput re-measure, force-push, title/body).
